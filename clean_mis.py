import argparse
import sys
from typing import List, Tuple
import re
import pandas as pd
from pathlib import Path

MONTH_FMT_TARGET = "%Y-%m-01"  # first-of-month ISO-like

HEADER_EXPECTED = [
    "Store Name", "Parameter", "Cafe Codes", "Region", "Category",
    "FOR SSG", "Area", "Store Start Date", "Vintage"
]

def find_header_row(df: pd.DataFrame) -> int:
    """
    Find the row index where the real headers live by matching expected labels.
    We accept a row if it contains at least 5 expected header tokens.
    """
    for i in range(min(10, len(df))):  # search near top
        row_vals = df.iloc[i].astype(str).tolist()
        hits = sum(1 for h in HEADER_EXPECTED if h in row_vals)
        if hits >= 5:
            return i
    # fallback: assume first row
    return 0

def normalize_colnames(cols: List[str]) -> List[str]:
    """
    Standardize known metadata columns. Leave month columns as-is (we'll parse).
    """
    mapping = {
        "Store Name": "store_name",
        "Parameter": "parameter",
        "Cafe Codes": "cafe_code",
        "Region": "region",
        "Category": "category",
        "FOR SSG": "for_ssg",
        "Area": "area_store",
        "Store Start Date": "store_start_date",
        "Vintage": "vintage",
    }
    out = []
    for c in cols:
        c2 = mapping.get(c, c)
        out.append(c2)
    return out

def is_month_label(s: str) -> bool:
    """
    Decide if a column label looks like a month column.
    Accepts 'Apr-21', 'Apr-2021', '2021-04-01 00:00:00', Excel-converted timestamps, etc.
    We'll try best-effort via pd.to_datetime later.
    """
    if s is None or s == "" or s.lower() in {"nan", "none"}:
        return False
    s = str(s).strip()
    # quick patterns
    if re.match(r"^[A-Za-z]{3}-\d{2,4}$", s):  # Apr-21 or Apr-2021
        return True
    # date-like tokens (ISO or excel-parsed string)
    try:
        pd.to_datetime(s, errors="raise")
        return True
    except Exception:
        return False

def parse_month_label(s: str) -> pd.Timestamp:
    """
    Convert a column label to first-of-month Timestamp.
    """
    s = str(s).strip()
    # common 'Apr-21' case
    for fmt in ["%b-%y", "%b-%Y"]:
        try:
            dt = pd.to_datetime(pd.to_datetime(s, format=fmt))
            return pd.Timestamp(year=dt.year, month=dt.month, day=1)
        except Exception:
            pass
    # generic parse
    try:
        dt = pd.to_datetime(s, errors="raise")
        return pd.Timestamp(year=dt.year, month=dt.month, day=1)
    except Exception:
        return pd.NaT

def clean_string(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s == "0":
        return None
    return s

def parse_numeric_value(raw, is_percent_param: bool) -> float:
    """
    Clean numeric 'value':
    - remove commas
    - if contains '%' OR parameter is percent, divide by 100
    - return float or None
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if s == "" or s.lower() in {"na", "nan", "none"}:
        return None
    # remove thousand separators
    s_clean = s.replace(",", "")
    is_percent_value = "%" in s_clean
    s_clean = s_clean.replace("%", "")
    # handle parentheses for negatives e.g., (123)
    if re.match(r"^\(\s*\d+(\.\d+)?\s*\)$", s_clean):
        s_clean = "-" + s_clean.strip("()").strip()
    try:
        val = float(s_clean)
    except Exception:
        return None
    if is_percent_param or is_percent_value:
        return val / 100.0
    return val

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Path to Excel file")
    ap.add_argument("--sheet", dest="sheet", default=None, help="Sheet name (optional)")
    ap.add_argument("--out", dest="out", required=True, help="Path to output CSV (long form)")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)

    # Load raw sheet without trusting headers
    df_raw = pd.read_excel(inp, sheet_name=args.sheet, header=None)
    hdr_row = find_header_row(df_raw)
    headers = df_raw.iloc[hdr_row].tolist()
    df = df_raw.iloc[hdr_row+1:].reset_index(drop=True)
    df.columns = [str(h) if not pd.isna(h) else "" for h in headers]

    # Drop fully-empty columns
    df = df.dropna(axis=1, how="all")

    # Normalize non-month column names
    df.columns = normalize_colnames(list(df.columns))

    # Identify month columns
    non_month_cols = [
        "store_name", "parameter", "cafe_code", "region",
        "category", "for_ssg", "area_store", "store_start_date", "vintage"
    ]
    month_cols = [c for c in df.columns if c not in non_month_cols]

    # Some month columns might still carry 'Unnamed: NN' strings if the header row was odd;
    # we only keep columns that look like real months
    month_cols = [c for c in month_cols if is_month_label(c)]

    # Parse store-level fields
    df["store_name"] = df["store_name"].apply(clean_string)
    df["parameter"] = df["parameter"].apply(clean_string)
    for col in ["cafe_code", "region", "category", "for_ssg", "vintage"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_string)

    # Clean store_start_date to date (if present)
    if "store_start_date" in df.columns:
        df["store_start_date"] = pd.to_datetime(df["store_start_date"], errors="coerce")

    # Melt months into long
    id_vars = [c for c in non_month_cols if c in df.columns]
    df_long = df.melt(id_vars=id_vars, value_vars=month_cols,
                      var_name="month_raw", value_name="value_raw")

    # Parse month labels to first-of-month
    df_long["month"] = df_long["month_raw"].apply(parse_month_label)

    # Clean numeric values; detect percent parameter
    # Your dataset uses a row-level parameter named "%" for margin.
    df_long["is_percent_param"] = df_long["parameter"].fillna("").astype(str).str.strip().eq("%")
    df_long["value"] = df_long.apply(
        lambda r: parse_numeric_value(r["value_raw"], bool(r["is_percent_param"])),
        axis=1
    )

    # Final tidy frame
    keep_cols = [
        "store_name", "parameter", "cafe_code", "region", "category",
        "for_ssg", "area_store", "store_start_date", "vintage", "month", "value"
    ]
    # Some rows may have NaT month or completely empty store/parameter—drop them
    tidy = df_long[keep_cols].copy()
    tidy = tidy[tidy["store_name"].notna() & tidy["parameter"].notna() & tidy["month"].notna()]

    # Sort for readability
    tidy = tidy.sort_values(["store_name", "parameter", "month"]).reset_index(drop=True)

    # Write CSV (UTF-8)
    tidy.to_csv(outp, index=False)

    # Also emit a DuckDB loader SQL
    duck_sql = f"""-- Create + load tidy long table
CREATE TABLE IF NOT EXISTS mis_long (
  store_name TEXT,
  parameter  TEXT,
  cafe_code  TEXT,
  region     TEXT,
  category   TEXT,
  for_ssg    TEXT,
  area_store BIGINT,
  store_start_date DATE,
  vintage    TEXT,
  month      DATE,
  value      DOUBLE
);

-- Adjust path as needed:
COPY mis_long FROM '{outp.as_posix()}' (HEADER, AUTO_DETECT TRUE);

-- Example: revenue by region in 2024
-- SELECT region, SUM(value) AS revenue
-- FROM mis_long
-- WHERE parameter = 'Revenue' AND month BETWEEN '2024-01-01' AND '2024-12-31'
-- GROUP BY region
-- ORDER BY revenue DESC;

-- Example: EBITDA margin (if you later store as parameter '%')
-- SELECT store_name, AVG(value) AS avg_margin
-- FROM mis_long
-- WHERE parameter = '%'
-- GROUP BY store_name
-- ORDER BY avg_margin DESC;
"""
    (outp.parent / "duckdb_load.sql").write_text(duck_sql, encoding="utf-8")

    print(f"✅ Wrote {len(tidy):,} rows to {outp}")
    print(f"🦆 Wrote DuckDB loader SQL to {(outp.parent / 'duckdb_load.sql').as_posix()}")

if __name__ == "__main__":
    pd.options.mode.copy_on_write = True
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)