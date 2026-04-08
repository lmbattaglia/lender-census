#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 19:39:44 2026

@author: loganbattaglia
"""

# Claude script for census! 

"""
fetch_s2301_full.py
--------------------
Fetches ALL estimate variables from ACS 5-Year Estimates Table S2301
(Employment Status) for years 2014-2024, across five geographies.
 
OUTPUT STRUCTURE
  One Excel workbook: s2301_full.xlsx  (single sheet)
  Rows  : one per geography-year combination (55 rows: 5 geos x 11 years)
  Col A : Geography
  Col B : Year
  Col C+: One column per S2301 estimate variable, header = human-readable label
 
  Transparent (no fill) cell backgrounds; thin black borders on all cells.
 
GEOGRAPHIES
  City of Syracuse, NY   place FIPS 73000, state 36
  Onondaga County, NY    county FIPS 067, state 36
  Central New York       aggregated from 5 counties (see CNY NOTE)
  New York State         state FIPS 36
  United States          national level
 
CNY NOTE -- AGGREGATION METHOD AND LIMITATIONS
  Central New York is not an official Census geography. Count variables
  (C01) are summed across Cayuga (011), Cortland (023), Madison (053),
  Onondaga (067), and Oswego (075) counties. Rate variables (C02, C03,
  C04) are simple unweighted averages of the five county values. A true
  aggregate rate requires dividing summed numerator counts by summed
  denominator counts, which cannot be reconstructed from derived
  percentages alone. Onondaga County (~60% of the regional labor force)
  dominates, so the simple average slightly over-weights the four
  smaller counties for rate variables.
 
2014 SCHEMA NOTE
  The 2014 ACS 5-year Subject Table API has a smaller S2301 schema than
  later years. The script discovers the valid variable list for each year
  at runtime by querying the /groups/S2301.json endpoint before fetching
  data, so it only requests variables that actually exist for that year.
  Variables absent from a given year appear as blank cells in the output.
 
REQUIREMENTS
  pip install requests pandas openpyxl
 
USAGE
  python fetch_s2301_full.py
  Enter your Census API key when prompted.
  Free key: https://api.census.gov/data/key_signup.html
"""
 
import sys
import time
import requests
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
 
# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
 
YEARS = list(range(2024, 2013, -1))   # 2024 down to 2014
 
STATE_FIPS     = "36"
SYRACUSE_PLACE = "73000"
ONONDAGA_FIPS  = "067"
 
CNY_COUNTIES = {
    "Cayuga County":   "011",
    "Cortland County": "023",
    "Madison County":  "053",
    "Onondaga County": "067",
    "Oswego County":   "075",
}
 
GEOGRAPHIES = [
    "City of Syracuse",
    "Onondaga County",
    "Central New York",
    "New York State",
    "United States",
]
 
BASE_URL = "https://api.census.gov/data/{year}/acs/acs5/subject"
 
# Census sentinel values that represent suppressed / unavailable data
SENTINELS = {-666666666.0, -555555555.0, -333333333.0,
             -222222222.0, -888888888.0, -999999999.0}
 
# Maximum variables per Census API call (hard API limit is 50)
MAX_PER_CALL = 45
 
# Courtesy pause between API calls
API_PAUSE = 0.05
 
# Full list of all possible estimate variable codes across all years
# (001-035 per column group; earlier years may only support a subset)
ALL_VAR_CODES = [
    f"S2301_{grp}_{row:03d}E"
    for grp in ("C01", "C02", "C03", "C04")
    for row in range(1, 36)
]
 
COL_GROUP_NAMES = {
    "C01": "Total (Count)",
    "C02": "Labor Force Participation Rate (%)",
    "C03": "Employment-Population Ratio (%)",
    "C04": "Unemployment Rate (%)",
}
 
# ---------------------------------------------------------------------------
# STEP 1: DISCOVER VALID VARIABLES PER YEAR
# ---------------------------------------------------------------------------
 
def fetch_year_variables(year: int, api_key: str) -> tuple[list, dict]:
    """
    Query the S2301 group metadata endpoint for a specific year.
    Returns:
      valid_vars : list of estimate variable codes that exist for this year
      labels     : dict {code: human-readable label}
 
    This is the fix for the 2014 400 errors: earlier years had fewer
    row indices in S2301 (e.g. no _021E through _035E). Requesting a
    variable that doesn't exist in that year's schema causes HTTP 400
    for the entire batch. By discovering valid vars first, we only
    request what actually exists.
    """
    url = f"https://api.census.gov/data/{year}/acs/acs5/subject/groups/S2301.json"
    try:
        resp = requests.get(url, params={"key": api_key}, timeout=30)
        resp.raise_for_status()
        raw = resp.json().get("variables", {})
    except Exception as e:
        print(f"  [WARNING] Could not fetch {year} variable list: {e}",
              file=sys.stderr)
        print(f"  [WARNING] Falling back to full variable list for {year}",
              file=sys.stderr)
        raw = {}
 
    labels = {}
    valid_set = set()
    for code, meta in raw.items():
        # Keep only estimate variables (end in E but not EA)
        if not (code.startswith("S2301_") and code.endswith("E")
                and not code.endswith("EA")):
            continue
        valid_set.add(code)
        label = meta.get("label", code)
        label = label.replace("Estimate!!", "").replace("!!", " -- ")
        # Older years use a different label format: "Total!!Estimate!!..."
        label = label.replace("Total!!Estimate!!", "")
        labels[code] = label
 
    # Filter our master list to only codes valid for this year,
    # preserving the canonical order
    if valid_set:
        valid_vars = [v for v in ALL_VAR_CODES if v in valid_set]
    else:
        # Fallback: try all codes and let parse_row handle missing ones
        valid_vars = ALL_VAR_CODES
 
    return valid_vars, labels
 
 
# ---------------------------------------------------------------------------
# STEP 2: API HELPERS
# ---------------------------------------------------------------------------
 
def api_get(url: str, params: dict) -> list | None:
    """GET the Census API; return parsed JSON or None on failure."""
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  [WARNING] HTTP {resp.status_code}: {resp.url[:120]}",
                  file=sys.stderr)
            return None
        return resp.json()
    except Exception as e:
        print(f"  [WARNING] Request error: {e}", file=sys.stderr)
        return None
 
 
def parse_row(data: list | None, var_codes: list) -> dict:
    """
    Extract values from a Census API response (header + one data row).
    Returns {variable_code: float | None}.
    Sentinel and suppressed values become None.
    """
    result = {v: None for v in var_codes}
    if not data or len(data) < 2:
        return result
    header = data[0]
    row    = data[1]
    for code in var_codes:
        if code not in header:
            continue
        raw = row[header.index(code)]
        if raw is None:
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if val not in SENTINELS:
            result[code] = val
    return result
 
 
def fetch_geo(year: int, geo_params: dict, valid_vars: list, api_key: str) -> dict:
    """
    Fetch all valid_vars for one geography in one year, batching into
    groups of MAX_PER_CALL. The api_key is passed separately and never
    mutated into geo_params, preventing parameter leakage between batches.
 
    NOTE: This is the other half of the 2014 fix. The previous version
    merged geo_params and the 'get' key together, which caused geo params
    from one geography to leak into the next batch's URL. Now each batch
    constructs a fresh params dict.
    """
    result = {}
    base_url = BASE_URL.format(year=year)
 
    for i in range(0, len(valid_vars), MAX_PER_CALL):
        batch = valid_vars[i : i + MAX_PER_CALL]
        # Build a clean params dict for every batch — never reuse/mutate
        params = {
            "get": ",".join(batch),
            "key": api_key,
            **geo_params,          # for=, in= (read-only spread, no mutation)
        }
        data = api_get(base_url, params)
        result.update(parse_row(data, batch))
        time.sleep(API_PAUSE)
 
    return result
 
 
# ---------------------------------------------------------------------------
# STEP 3: PER-GEOGRAPHY FETCH FUNCTIONS
# ---------------------------------------------------------------------------
 
def fetch_syracuse(year: int, valid_vars: list, api_key: str) -> dict:
    return fetch_geo(
        year,
        {"for": f"place:{SYRACUSE_PLACE}", "in": f"state:{STATE_FIPS}"},
        valid_vars,
        api_key,
    )
 
 
def fetch_onondaga(year: int, valid_vars: list, api_key: str) -> dict:
    return fetch_geo(
        year,
        {"for": f"county:{ONONDAGA_FIPS}", "in": f"state:{STATE_FIPS}"},
        valid_vars,
        api_key,
    )
 
 
def fetch_cny(year: int, valid_vars: list, api_key: str) -> dict:
    """
    Aggregate five CNY counties.
    C01 (counts) -> summed; C02/C03/C04 (rates) -> simple average.
    """
    county_data = []
    for county_name, fips in CNY_COUNTIES.items():
        row = fetch_geo(
            year,
            {"for": f"county:{fips}", "in": f"state:{STATE_FIPS}"},
            valid_vars,
            api_key,
        )
        county_data.append(row)
        print(f"      {county_name} fetched", flush=True)
 
    result = {}
    for var in valid_vars:
        col_group = var[6:9]   # "C01", "C02", "C03", or "C04"
        values = [r[var] for r in county_data if r.get(var) is not None]
        if not values:
            result[var] = None
        elif col_group == "C01":
            result[var] = sum(values)
        else:
            result[var] = sum(values) / len(values)
    return result
 
 
def fetch_new_york(year: int, valid_vars: list, api_key: str) -> dict:
    return fetch_geo(year, {"for": f"state:{STATE_FIPS}"}, valid_vars, api_key)
 
 
def fetch_us(year: int, valid_vars: list, api_key: str) -> dict:
    return fetch_geo(year, {"for": "us:1"}, valid_vars, api_key)
 
 
FETCH_FUNCS = {
    "City of Syracuse": fetch_syracuse,
    "Onondaga County":  fetch_onondaga,
    "Central New York": fetch_cny,
    "New York State":   fetch_new_york,
    "United States":    fetch_us,
}
 
# ---------------------------------------------------------------------------
# STEP 4: DATA COLLECTION
# ---------------------------------------------------------------------------
 
def collect_data(api_key: str) -> tuple[list[dict], dict, list]:
    """
    Returns:
      rows       : list of row dicts, one per (geography, year) combination
      all_labels : merged label dict across all years
      all_vars   : union of valid variable codes across all years, in order
    """
    rows: list[dict] = []
    all_labels: dict = {}
    seen_vars: set   = set()
    ordered_vars: list = []   # preserves canonical order
 
    for year in YEARS:
        print(f"\nYear {year} — discovering variables ...", flush=True)
        valid_vars, labels = fetch_year_variables(year, api_key)
        all_labels.update(labels)
 
        # Track union of all variable codes seen, in canonical order
        for v in valid_vars:
            if v not in seen_vars:
                ordered_vars.append(v)
                seen_vars.add(v)
 
        print(f"  {len(valid_vars)} variables available for {year}")
 
        for geo in GEOGRAPHIES:
            print(f"  {geo} ...", flush=True)
            values = FETCH_FUNCS[geo](year, valid_vars, api_key)
            row = {"Geography": geo, "Year": year}
            row.update(values)
            rows.append(row)
 
    return rows, all_labels, ordered_vars
 
 
# ---------------------------------------------------------------------------
# STEP 5: EXCEL OUTPUT
# ---------------------------------------------------------------------------
 
# Styling
HDR_FONT  = Font(name="Arial", bold=True, size=9)
DAT_FONT  = Font(name="Arial", size=9)
NOTE_FONT = Font(name="Arial", italic=True, size=8, color="444444")
 
NO_FILL   = PatternFill(fill_type=None)   # transparent / no fill
 
CENTER    = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT      = Alignment(horizontal="left",   vertical="center", wrap_text=True)
 
BLK       = Side(style="thin", color="000000")
BLK_BDR   = Border(left=BLK, right=BLK, top=BLK, bottom=BLK)
 
 
def build_workbook(rows: list[dict], labels: dict, ordered_vars: list,
                   output_path: str):
    """
    Write one sheet:
      Row 1 : column headers
      Rows 2+: one row per (geography, year) pair
      Columns: Geography | Year | [one per variable, labeled]
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "S2301 Employment Status"
 
    # ── Column header labels ──────────────────────────────────────────────────
    # Col A = Geography, Col B = Year, Col C onwards = variables
    fixed_headers = ["Geography", "Year"]
    var_headers   = [labels.get(v, v) for v in ordered_vars]
    all_headers   = fixed_headers + var_headers
    total_cols    = len(all_headers)
 
    for ci, h in enumerate(all_headers, start=1):
        cell = ws.cell(row=1, column=ci)
        cell.value         = h
        cell.font          = HDR_FONT
        cell.fill          = NO_FILL
        cell.alignment     = CENTER
        cell.border        = BLK_BDR
 
    ws.row_dimensions[1].height = 60   # tall header for wrapped text
 
    # ── Data rows ─────────────────────────────────────────────────────────────
    for ri, row_data in enumerate(rows, start=2):
        geo  = row_data["Geography"]
        year = row_data["Year"]
 
        # Col A: Geography
        ca = ws.cell(row=ri, column=1)
        ca.value     = geo
        ca.font      = HDR_FONT
        ca.fill      = NO_FILL
        ca.alignment = LEFT
        ca.border    = BLK_BDR
 
        # Col B: Year
        cb = ws.cell(row=ri, column=2)
        cb.value         = year
        cb.font          = DAT_FONT
        cb.fill          = NO_FILL
        cb.alignment     = CENTER
        cb.border        = BLK_BDR
        cb.number_format = "0"
 
        # Col C+: variable values
        for ci, var in enumerate(ordered_vars, start=3):
            val      = row_data.get(var)
            col_grp  = var[6:9]
            is_rate  = col_grp in ("C02", "C03", "C04")
            num_fmt  = "0.0" if is_rate else "#,##0"
 
            cell = ws.cell(row=ri, column=ci)
            cell.value         = val
            cell.font          = DAT_FONT
            cell.fill          = NO_FILL
            cell.alignment     = CENTER
            cell.border        = BLK_BDR
            cell.number_format = num_fmt
 
        ws.row_dimensions[ri].height = 14
 
    # ── Column widths ─────────────────────────────────────────────────────────
    ws.column_dimensions["A"].width = 20
    ws.column_dimensions["B"].width = 7
    for ci in range(3, total_cols + 1):
        ws.column_dimensions[get_column_letter(ci)].width = 13
 
    # ── Freeze Geography + Year columns and header row ────────────────────────
    ws.freeze_panes = "C2"
 
    # ── Source note below data ────────────────────────────────────────────────
    note_row = len(rows) + 3
    ws.merge_cells(f"A{note_row}:{get_column_letter(total_cols)}{note_row}")
    n1 = ws.cell(row=note_row, column=1)
    n1.value = (
        "Source: U.S. Census Bureau, American Community Survey 5-Year Estimates, "
        "Table S2301 (Employment Status). Variables S2301_C01_001E\u2013S2301_C04_035E "
        "(subset varies by year; earlier releases had fewer row indices)."
    )
    n1.font      = NOTE_FONT
    n1.alignment = LEFT
 
    note_row2 = note_row + 1
    ws.merge_cells(f"A{note_row2}:{get_column_letter(total_cols)}{note_row2}")
    n2 = ws.cell(row=note_row2, column=1)
    n2.value = (
        "Central New York = Cayuga, Cortland, Madison, Onondaga, and Oswego counties. "
        "C01 count variables are summed; C02\u2013C04 rate variables are simple "
        "unweighted county averages (see script docstring for weighting limitations)."
    )
    n2.font      = NOTE_FONT
    n2.alignment = LEFT
 
    wb.save(output_path)
    print(f"\nSaved: {output_path}")
 
 
# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
 
def main():
    print("=" * 65)
    print("ACS 5-Year S2301 Full Table Fetcher")
    print("Variables : S2301_C01_001E - S2301_C04_035E (subset per year)")
    print("Years     : 2014-2024  (11 releases)")
    print("Geos      : City of Syracuse | Onondaga County | Central New York")
    print("            New York State   | United States")
    print("Output    : single sheet, geo+year rows x variable columns")
    print("=" * 65)
 
    api_key = input("\nEnter your Census API key: ").strip()
    if not api_key:
        print("ERROR: No API key provided. Exiting.")
        sys.exit(1)
 
    print("\nCollecting data (this will take several minutes) ...", flush=True)
    rows, labels, ordered_vars = collect_data(api_key)
 
    output_path = "s2301_full.xlsx"
    print("\nBuilding workbook ...", flush=True)
    build_workbook(rows, labels, ordered_vars, output_path)
 
    print("\n--- Complete ---")
    print(f"Output  : {output_path}")
    print(f"Sheet   : 1 (S2301 Employment Status)")
    print(f"Rows    : {len(rows)} data rows ({len(GEOGRAPHIES)} geos x {len(YEARS)} years)")
    print(f"Columns : 2 fixed (Geography, Year) + {len(ordered_vars)} variable columns")
 
 
if __name__ == "__main__":
    main()