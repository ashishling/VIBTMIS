#!/usr/bin/env python3
"""
Robust CSV processor for BTC store data in cross-tab format.
Converts cross-tabulated data to tidy long format for DuckDB/SQLite analysis.

Usage:
    python process_btc_csv.py --input "BTC store for CSV.csv" --output "clean_mis_long.csv"
"""

import argparse
import sys
import re
import pandas as pd
from pathlib import Path
from typing import List, Optional, Union
import warnings

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

def find_header_row(df: pd.DataFrame) -> int:
    """
    Find the row index where the real headers are located.
    Looks for expected header patterns in the first few rows.
    """
    expected_headers = [
        "Store Name", "Parameter", "Cafe Codes", "Region", "Category",
        "FOR SSG", "Area", "Store Start Date", "Vintage"
    ]
    
    for i in range(min(10, len(df))):
        row_values = df.iloc[i].astype(str).tolist()
        # Count how many expected headers we find in this row
        matches = sum(1 for header in expected_headers if header in row_values)
        if matches >= 5:  # Require at least 5 matches to be confident
            return i
    
    # Fallback: assume first row if no clear header found
    return 0

def normalize_column_names(columns: List[str]) -> List[str]:
    """
    Convert column names to snake_case for key metadata columns.
    Leave month columns unchanged for later processing.
    """
    column_mapping = {
        "Store Name": "store_name",
        "Parameter": "parameter", 
        "Cafe Codes": "cafe_code",
        "Region": "region",
        "Category": "category",
        "FOR SSG": "for_ssg",
        "Area": "area_store",
        "Store Start Date": "store_start_date",
        "Vintage": "vintage"
    }
    
    normalized = []
    for col in columns:
        normalized.append(column_mapping.get(col, col))
    
    return normalized

def is_month_column(column_name: str) -> bool:
    """
    Determine if a column represents a month/date.
    Handles various formats: Apr-21, Apr-2021, Excel date serials, etc.
    """
    if pd.isna(column_name) or column_name == "" or str(column_name).lower() in {"nan", "none"}:
        return False
    
    col_str = str(column_name).strip()
    
    # Pattern for MMM-YY or MMM-YYYY format
    if re.match(r"^[A-Za-z]{3}-\d{2,4}$", col_str):
        return True
    
    # Try to parse as date - if successful, it's likely a month column
    try:
        pd.to_datetime(col_str, errors="raise")
        return True
    except (ValueError, TypeError):
        pass
    
    # Check for Excel date serial numbers (numeric strings)
    try:
        if col_str.replace(".", "").isdigit():
            # Convert Excel serial to date to verify
            excel_date = pd.to_datetime(float(col_str), unit='D', origin='1899-12-30')
            return True
    except (ValueError, TypeError):
        pass
    
    return False

def parse_month_column(column_name: str) -> pd.Timestamp:
    """
    Convert a month column name to a first-of-month timestamp.
    Returns NaT if parsing fails.
    """
    col_str = str(column_name).strip()
    
    # Handle MMM-YY format (e.g., Apr-21)
    for fmt in ["%b-%y", "%b-%Y"]:
        try:
            dt = pd.to_datetime(col_str, format=fmt)
            return pd.Timestamp(year=dt.year, month=dt.month, day=1)
        except (ValueError, TypeError):
            continue
    
    # Handle Excel date serial numbers
    try:
        if col_str.replace(".", "").isdigit():
            excel_date = pd.to_datetime(float(col_str), unit='D', origin='1899-12-30')
            return pd.Timestamp(year=excel_date.year, month=excel_date.month, day=1)
    except (ValueError, TypeError):
        pass
    
    # Generic date parsing
    try:
        dt = pd.to_datetime(col_str, errors="raise")
        return pd.Timestamp(year=dt.year, month=dt.month, day=1)
    except (ValueError, TypeError):
        return pd.NaT

def clean_string_value(value: Union[str, float, int]) -> Optional[str]:
    """
    Clean string values: trim whitespace, convert empty/zero strings to None.
    """
    if pd.isna(value):
        return None
    
    str_val = str(value).strip()
    
    # Convert empty strings and "0" to None for categorical fields
    if str_val == "" or str_val == "0":
        return None
    
    return str_val

def parse_numeric_value(raw_value: Union[str, float, int], is_percent_parameter: bool = False) -> Optional[float]:
    """
    Parse numeric values with robust handling of:
    - Comma separators
    - Percentage values (both in parameter name and value)
    - Empty/null values
    - Parentheses for negative numbers
    """
    if pd.isna(raw_value):
        return None
    
    str_val = str(raw_value).strip()
    
    # Handle empty or null-like values
    if str_val == "" or str_val.lower() in {"na", "nan", "none", "--", "closed"}:
        return None
    
    # Remove thousand separators
    cleaned = str_val.replace(",", "")
    
    # Check if value contains percentage sign
    is_percent_value = "%" in cleaned
    cleaned = cleaned.replace("%", "")
    
    # Handle parentheses for negative numbers (e.g., "(123.45)")
    if re.match(r"^\(\s*\d+(\.\d+)?\s*\)$", cleaned):
        cleaned = "-" + cleaned.strip("()").strip()
    
    # Convert to float
    try:
        numeric_value = float(cleaned)
    except (ValueError, TypeError):
        return None
    
    # Convert percentage to decimal if needed
    if is_percent_parameter or is_percent_value:
        return numeric_value / 100.0
    
    return numeric_value

def main():
    parser = argparse.ArgumentParser(
        description="Process BTC store CSV from cross-tab to tidy long format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_btc_csv.py --input "BTC store for CSV.csv" --output "clean_mis_long.csv"
  python process_btc_csv.py -i data.csv -o output.csv --verbose
        """
    )
    
    parser.add_argument(
        "--input", "-i", 
        required=True, 
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--output", "-o", 
        required=True, 
        help="Path to output CSV file (long format)"
    )
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"ERROR: Input file '{input_path}' not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        if args.verbose:
            print(f"📖 Reading CSV file: {input_path}")
        
        # Read CSV without assuming headers
        df_raw = pd.read_csv(input_path, header=None, dtype=str)
        
        if args.verbose:
            print(f"📊 Raw data shape: {df_raw.shape}")
        
        # Find the header row
        header_row_idx = find_header_row(df_raw)
        
        if args.verbose:
            print(f"🔍 Found header row at index: {header_row_idx}")
        
        # Extract headers and data
        headers = df_raw.iloc[header_row_idx].tolist()
        df_data = df_raw.iloc[header_row_idx + 1:].reset_index(drop=True)
        df_data.columns = [str(h) if not pd.isna(h) else f"Unnamed_{i}" for i, h in enumerate(headers)]
        
        # Remove completely empty columns
        df_data = df_data.dropna(axis=1, how="all")
        
        if args.verbose:
            print(f"📋 Columns after cleanup: {list(df_data.columns)}")
        
        # Normalize column names
        df_data.columns = normalize_column_names(list(df_data.columns))
        
        # Identify month columns
        metadata_columns = [
            "store_name", "parameter", "cafe_code", "region", 
            "category", "for_ssg", "area_store", "store_start_date", "vintage"
        ]
        
        month_columns = [col for col in df_data.columns if col not in metadata_columns]
        month_columns = [col for col in month_columns if is_month_column(col)]
        
        if args.verbose:
            print(f"📅 Found {len(month_columns)} month columns: {month_columns[:5]}{'...' if len(month_columns) > 5 else ''}")
        
        # Clean metadata columns
        for col in ["store_name", "parameter"]:
            if col in df_data.columns:
                df_data[col] = df_data[col].apply(clean_string_value)
        
        for col in ["cafe_code", "region", "category", "for_ssg", "vintage"]:
            if col in df_data.columns:
                df_data[col] = df_data[col].apply(clean_string_value)
        
        # Parse store start date
        if "store_start_date" in df_data.columns:
            df_data["store_start_date"] = pd.to_datetime(df_data["store_start_date"], errors="coerce")
        
        # Parse area_store as numeric
        if "area_store" in df_data.columns:
            df_data["area_store"] = df_data["area_store"].apply(
                lambda x: parse_numeric_value(x) if pd.notna(x) else None
            )
        
        if args.verbose:
            print("🔄 Melting data to long format...")
        
        # Melt to long format
        id_vars = [col for col in metadata_columns if col in df_data.columns]
        df_long = df_data.melt(
            id_vars=id_vars,
            value_vars=month_columns,
            var_name="month_raw",
            value_name="value_raw"
        )
        
        if args.verbose:
            print(f"📏 Long format shape: {df_long.shape}")
        
        # Parse month columns to timestamps
        df_long["month"] = df_long["month_raw"].apply(parse_month_column)
        
        # Clean numeric values
        df_long["is_percent_parameter"] = (
            df_long["parameter"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("%")
        )
        
        df_long["value"] = df_long.apply(
            lambda row: parse_numeric_value(
                row["value_raw"], 
                bool(row["is_percent_parameter"])
            ),
            axis=1
        )
        
        # Create final tidy dataset
        final_columns = [
            "store_name", "parameter", "cafe_code", "region", "category",
            "for_ssg", "area_store", "store_start_date", "vintage", "month", "value"
        ]
        
        df_tidy = df_long[final_columns].copy()
        
        # Remove rows with missing essential data
        df_tidy = df_tidy[
            df_tidy["store_name"].notna() & 
            df_tidy["parameter"].notna() & 
            df_tidy["month"].notna()
        ]
        
        # Sort for readability
        df_tidy = df_tidy.sort_values(["store_name", "parameter", "month"]).reset_index(drop=True)
        
        if args.verbose:
            print(f"✅ Final tidy dataset shape: {df_tidy.shape}")
            print(f"📊 Unique stores: {df_tidy['store_name'].nunique()}")
            print(f"📊 Unique parameters: {df_tidy['parameter'].unique()}")
            print(f"📊 Date range: {df_tidy['month'].min()} to {df_tidy['month'].max()}")
        
        # Write output CSV with proper escaping for store names with commas
        df_tidy.to_csv(output_path, index=False, encoding="utf-8", quoting=1)  # quoting=1 means quote all fields
        
        # Generate DuckDB SQL script
        sql_script = f"""-- DuckDB table creation and data loading script
-- Generated for BTC store MIS data

-- Create the table with appropriate data types
CREATE TABLE IF NOT EXISTS mis_long (
    store_name TEXT,
    parameter TEXT,
    cafe_code TEXT,
    region TEXT,
    category TEXT,
    for_ssg TEXT,
    area_store DOUBLE,
    store_start_date DATE,
    vintage TEXT,
    month DATE,
    value DOUBLE
);

-- Load data from CSV
COPY mis_long FROM '{output_path.absolute()}' (HEADER, AUTO_DETECT TRUE);

-- Verify data loaded correctly
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT store_name) as unique_stores,
    COUNT(DISTINCT parameter) as unique_parameters,
    MIN(month) as earliest_month,
    MAX(month) as latest_month
FROM mis_long;

-- Example queries:

-- 1. Revenue by region for 2024
-- SELECT 
--     region,
--     SUM(value) as total_revenue
-- FROM mis_long 
-- WHERE parameter = 'Revenue' 
--   AND month BETWEEN '2024-01-01' AND '2024-12-31'
-- GROUP BY region 
-- ORDER BY total_revenue DESC;

-- 2. Average EBITDA margin by store
-- SELECT 
--     store_name,
--     AVG(value) as avg_ebitda_margin
-- FROM mis_long 
-- WHERE parameter = '%'
-- GROUP BY store_name 
-- ORDER BY avg_ebitda_margin DESC;

-- 3. Monthly transaction trends
-- SELECT 
--     month,
--     SUM(value) as total_transactions
-- FROM mis_long 
-- WHERE parameter = 'Transactions'
-- GROUP BY month 
-- ORDER BY month;

-- 4. Store performance comparison
-- SELECT 
--     store_name,
--     region,
--     category,
--     SUM(CASE WHEN parameter = 'Revenue' THEN value ELSE 0 END) as revenue,
--     SUM(CASE WHEN parameter = 'EBITDA' THEN value ELSE 0 END) as ebitda,
--     AVG(CASE WHEN parameter = '%' THEN value ELSE NULL END) as margin
-- FROM mis_long 
-- WHERE month >= '2024-01-01'
-- GROUP BY store_name, region, category
-- ORDER BY revenue DESC;
"""
        
        sql_path = output_path.parent / "duckdb_load.sql"
        sql_path.write_text(sql_script, encoding="utf-8")
        
        print(f"✅ Successfully processed {len(df_tidy):,} rows")
        print(f"📄 Output CSV: {output_path}")
        print(f"🦆 DuckDB SQL: {sql_path}")
        
        if args.verbose:
            print(f"\n📈 Data Summary:")
            print(f"   • Total rows: {len(df_tidy):,}")
            print(f"   • Unique stores: {df_tidy['store_name'].nunique()}")
            print(f"   • Unique parameters: {df_tidy['parameter'].nunique()}")
            print(f"   • Date range: {df_tidy['month'].min().strftime('%Y-%m')} to {df_tidy['month'].max().strftime('%Y-%m')}")
            print(f"   • Parameters: {', '.join(df_tidy['parameter'].unique())}")
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
