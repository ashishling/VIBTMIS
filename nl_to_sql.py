#!/usr/bin/env python3
"""
Natural Language to SQL Query Generator for BTC Store Data
Uses OpenAI API to convert natural language questions to SQL queries for DuckDB.

Usage:
    python nl_to_sql.py "What are the top 10 stores by revenue in 2024?"
    python nl_to_sql.py "Show me monthly transaction trends"
"""

import argparse
import json
import sys
from pathlib import Path
import subprocess
import os
from typing import Dict, List, Optional
import openai
from dotenv import load_dotenv
from supabase_client import execute_sql_query as supabase_execute_query
 do 
# Load environment variables from .env file
load_dotenv()

# Database schema and context for the LLM
DATABASE_SCHEMA = """
Table: mis_long
Columns:
- store_name (TEXT): Name of the store
- parameter (TEXT): Type of metric (Area, Revenue, EBITDA, Transactions, COGS, Electricity, Gross Margin, Gross Profit, Others, People Cost, Rent, Revenue/Sq. Ft., Sales Commission, Avg Size of Transactions)
- cafe_code (TEXT): Store code identifier
- region (TEXT): Geographic region (Delhi, Mumbai, Bangalore, Gurgaon, Pune, Noida, etc.)
- category (TEXT): Store category (CWK, SIS/Others)
- for_ssg (TEXT): SSG flag
- area_store (DOUBLE): Store area in square feet
- store_start_date (DATE): Store opening date
- vintage (TEXT): Store vintage (FY18, FY19, etc.)
- month (DATE): Month (first of month, format: YYYY-MM-01)
- value (DOUBLE): Numeric value for the metric

Key Metrics:
- Revenue: Total revenue for the store
- EBITDA: Earnings before interest, taxes, depreciation, and amortization
- Transactions: Number of transactions
- Area: Store area in square feet
- COGS: Cost of goods sold
- Rent: Rent expenses
- People Cost: Employee costs
- Electricity: Electricity expenses
- Gross Margin: Gross profit margin
- Revenue/Sq. Ft.: Revenue per square foot
- Sales Commission: Commission expenses

Date Range: April 2021 to July 2025
Total Stores: 198 stores across multiple regions
"""

EXAMPLE_QUERIES = """
Example queries and their SQL:

1. "What are the top 10 stores by revenue in 2024?"
SELECT 
    store_name,
    region,
    SUM(value) as total_revenue
FROM mis_long 
WHERE parameter = 'Revenue' 
  AND month BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY store_name, region
ORDER BY total_revenue DESC
LIMIT 10;

2. "Show me monthly transaction trends"
SELECT 
    month,
    SUM(value) as total_transactions
FROM mis_long 
WHERE parameter = 'Transactions'
GROUP BY month 
ORDER BY month;

3. "Which region has the highest EBITDA in 2024?"
SELECT 
    region,
    SUM(value) as total_ebitda
FROM mis_long 
WHERE parameter = 'EBITDA' 
  AND month BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY region 
ORDER BY total_ebitda DESC;

4. "What's the average store area by region?"
SELECT 
    region,
    AVG(area_store) as avg_area
FROM mis_long 
WHERE area_store IS NOT NULL
GROUP BY region
ORDER BY avg_area DESC;

5. "Show me revenue per square foot for each store"
SELECT 
    store_name,
    region,
    area_store,
    SUM(CASE WHEN parameter = 'Revenue' THEN value ELSE 0 END) as revenue,
    SUM(CASE WHEN parameter = 'Revenue' THEN value ELSE 0 END) / NULLIF(area_store, 0) as revenue_per_sqft
FROM mis_long 
WHERE month >= '2024-01-01'
  AND area_store IS NOT NULL
GROUP BY store_name, region, area_store
HAVING revenue > 0
ORDER BY revenue_per_sqft DESC;
"""

def get_openai_client() -> openai.OpenAI:
    """Initialize OpenAI client with API key from environment."""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError(
            "Please set your OpenAI API key as an environment variable:\n"
            "export OPENAI_API_KEY='your-api-key-here'"
        )
    return openai.OpenAI(api_key=api_key)

def generate_sql_query(natural_language_query: str, client: openai.OpenAI) -> str:
    """Generate SQL query from natural language using OpenAI API."""
    
    system_prompt = f"""You are a SQL expert specializing in retail store analytics. 
You have access to a DuckDB database with the following schema:

{DATABASE_SCHEMA}

{EXAMPLE_QUERIES}

Instructions:
1. Convert the user's natural language question into a precise SQL query
2. Always use proper SQL syntax for DuckDB
3. Use appropriate date filtering (the data spans 2021-2025)
4. Handle NULL values appropriately
5. Use meaningful column aliases
6. Return ONLY the SQL query, no explanations or markdown formatting
7. If the query asks for trends over time, group by month and order by month
8. If asking for top/best performers, use ORDER BY ... DESC LIMIT
9. Use SUM() for aggregating values, AVG() for averages
10. Always filter by parameter when looking for specific metrics"""

    user_prompt = f"""Convert this natural language question to SQL: {natural_language_query}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Using the more cost-effective model
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,  # Low temperature for more consistent SQL
            max_tokens=1000
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # Clean up any markdown formatting
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
        
    except Exception as e:
        raise Exception(f"Error generating SQL query: {e}")

def execute_sql_query(sql_query: str) -> str:
    """Execute SQL query using DuckDB and return results."""
    try:
        # Create the complete SQL command with LIMIT to prevent overwhelming output
        # First, let's check if the query already has a LIMIT
        sql_upper = sql_query.upper().strip()
        has_limit = 'LIMIT' in sql_upper
        
        # Add LIMIT if not present to prevent overwhelming output
        if not has_limit and not sql_upper.endswith(';'):
            sql_with_limit = sql_query + " LIMIT 1000;"
        elif not has_limit:
            sql_with_limit = sql_query.rstrip(';') + " LIMIT 1000;"
        else:
            sql_with_limit = sql_query
            
        full_sql = f"""
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
COPY mis_long FROM 'clean_mis_long.csv' (HEADER);
{sql_with_limit}
"""
        
        # Use DuckDB command line to execute the SQL directly with table output mode
        result = subprocess.run(
            ["duckdb", "-table", "-cmd", "SET max_rows_per_page=1000;", "-c", full_sql],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.returncode != 0:
            return f"Error executing query:\n{result.stderr}"
        
        output = result.stdout
        
        # Add row count information if the query doesn't already have a limit
        if not has_limit:
            # Try to get the actual count
            try:
                count_sql = f"""
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
COPY mis_long FROM 'clean_mis_long.csv' (HEADER);
SELECT COUNT(*) as total_rows FROM ({sql_query.rstrip(';')});
"""
                count_result = subprocess.run(
                    ["duckdb", "-table", "-cmd", "SET max_rows_per_page=1000;", "-c", count_sql],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent
                )
                
                if count_result.returncode == 0:
                    # Extract the count from the output
                    count_lines = count_result.stdout.strip().split('\n')
                    if len(count_lines) >= 3:  # Header + separator + data row
                        total_count = count_lines[-2].strip()  # Second to last line
                        if total_count.isdigit():
                            output += f"\n\n📊 Total matching rows: {total_count:,}"
                            if int(total_count) > 1000:
                                output += f"\n⚠️  Showing first 1,000 rows (limited for display)"
            except:
                pass  # If count query fails, just show the limited results
        
        return output
        
    except Exception as e:
        return f"Error executing query: {e}"

def main():
    parser = argparse.ArgumentParser(
        description="Convert natural language questions to SQL queries for BTC store data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nl_to_sql.py "What are the top 10 stores by revenue in 2024?"
  python nl_to_sql.py "Show me monthly transaction trends"
  python nl_to_sql.py "Which region has the highest EBITDA?"
  python nl_to_sql.py "What's the average store area by category?"
        """
    )
    
    parser.add_argument(
        "query",
        help="Natural language question about the data"
    )
    parser.add_argument(
        "--sql-only", "-s",
        action="store_true",
        help="Only generate SQL query, don't execute it"
    )
    parser.add_argument(
        "--api-key",
        help="OpenAI API key (or set OPENAI_API_KEY environment variable)"
    )
    
    args = parser.parse_args()
    
    try:
        # Set API key if provided
        if args.api_key:
            os.environ['OPENAI_API_KEY'] = args.api_key
        
        # Initialize OpenAI client
        client = get_openai_client()
        
        print(f"🤖 Converting query: '{args.query}'")
        print("⏳ Generating SQL...")
        
        # Generate SQL query
        sql_query = generate_sql_query(args.query, client)
        
        print("✅ Generated SQL query:")
        print("-" * 50)
        print(sql_query)
        print("-" * 50)
        
        if args.sql_only:
            print("📝 SQL query generated (not executed)")
            return
        
        print("\n🔄 Executing query...")
        
        # Execute the query
        result = execute_sql_query(sql_query)
        
        print("📊 Query Results:")
        print("=" * 50)
        print(result)
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
