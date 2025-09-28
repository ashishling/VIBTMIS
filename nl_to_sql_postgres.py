#!/usr/bin/env python3
"""
Natural Language to SQL Query Generator for BT Store Data
Uses OpenAI API to convert natural language questions to SQL queries for PostgreSQL.

Usage:
    python nl_to_sql_postgres.py "What are the top 10 stores by revenue in 2024?"
    python nl_to_sql_postgres.py "Show me monthly transaction trends"
"""

import argparse
import json
import sys
from pathlib import Path
import os
from typing import Dict, List, Optional
import openai
from dotenv import load_dotenv
from postgres_client import execute_sql_query as postgres_execute_query

# Load environment variables (optional for Railway deployment)
load_dotenv(override=False)

# Database schema and context for the LLM
DATABASE_SCHEMA = """
Table: mis_long
Columns:
- store_name (TEXT): Name of the store
- parameter (TEXT): Type of metric (Area, Revenue, EBITDA, Transactions, COGS, Electricity, Gross Margin, Gross Profit, Others, People Cost, Rent, Revenue/Sq. Ft., Sales Commission, Avg Size of Transactions)
- cafe_code (TEXT): Store code identifier
- region (TEXT): Geographic region of the store
- category (TEXT): Store category classification
- for_ssg (TEXT): SSG classification
- area_store (DOUBLE PRECISION): Store area in square feet
- store_start_date (DATE): Date when the store started operations
- vintage (TEXT): Store vintage/age classification
- month (DATE): Month for the data point (first day of month)
- value (DOUBLE PRECISION): The metric value for that store/month/parameter combination

Key insights about the data:
- Each row represents one metric value for one store in one month
- Parameters include financial metrics (Revenue, EBITDA, COGS), operational metrics (Transactions, Area), and efficiency metrics (Revenue/Sq. Ft.)
- Data spans from April 2021 to July 2025
- Some stores may have missing data for certain months or parameters
- Values can be positive (revenue, transactions) or negative (costs)
- Area is typically in square feet, revenue in currency units, transactions as counts
"""

def get_openai_client():
    """Initialize and return OpenAI client."""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables. Please set it in your .env file or Railway environment variables.")
    
    return openai.OpenAI(api_key=api_key)

def generate_sql_query(natural_query: str, openai_client: openai.OpenAI) -> str:
    """Generate SQL query from natural language using OpenAI."""
    
    system_prompt = f"""You are an expert SQL query generator for retail store analytics. 
    
Database Schema:
{DATABASE_SCHEMA}

Instructions:
1. Generate PostgreSQL-compatible SQL queries
2. Always use proper date handling for the 'month' column
3. For revenue/EBITDA queries, filter by parameter = 'Revenue' or parameter = 'EBITDA'
4. For transaction queries, filter by parameter = 'Transactions'
5. For area queries, filter by parameter = 'Area'
6. Use appropriate aggregations (SUM, AVG, COUNT) based on the question
7. Include proper WHERE clauses to filter by date ranges when relevant
8. Use LIMIT when appropriate to prevent overwhelming results
9. For time-based queries, use DATE_TRUNC or EXTRACT functions
10. Always include the most relevant columns in SELECT

Example queries:
- "Top stores by revenue": SELECT store_name, SUM(value) as total_revenue FROM mis_long WHERE parameter = 'Revenue' GROUP BY store_name ORDER BY total_revenue DESC LIMIT 10;
- "Monthly trends": SELECT month, SUM(value) as total_revenue FROM mis_long WHERE parameter = 'Revenue' GROUP BY month ORDER BY month;
- "Store performance": SELECT store_name, region, SUM(value) as revenue FROM mis_long WHERE parameter = 'Revenue' GROUP BY store_name, region ORDER BY revenue DESC;

Generate a single, well-formed SQL query for the user's question."""

    user_prompt = f"""Convert this natural language question to SQL: "{natural_query}"

Return only the SQL query, no explanations or markdown formatting."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # Clean up the SQL query
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
        
        return sql_query
        
    except Exception as e:
        raise Exception(f"Error generating SQL query: {e}")

def execute_sql_query(sql_query: str) -> str:
    """Execute SQL query using PostgreSQL and return results."""
    try:
        # Add LIMIT if not present to prevent overwhelming output
        sql_upper = sql_query.upper().strip()
        has_limit = 'LIMIT' in sql_upper
        
        if not has_limit and not sql_upper.endswith(';'):
            sql_with_limit = sql_query + " LIMIT 1000;"
        elif not has_limit:
            sql_with_limit = sql_query.rstrip(';') + " LIMIT 1000;"
        else:
            sql_with_limit = sql_query
        
        # Execute the query using PostgreSQL
        output = postgres_execute_query(sql_with_limit)
        
        # If no explicit LIMIT was added, try to get total count
        if not has_limit:
            try:
                # For simple queries, we can try to get a count
                if 'FROM mis_long' in sql_query.upper() and 'GROUP BY' not in sql_query.upper():
                    # Try to get count for simple SELECT queries
                    count_query = f"SELECT COUNT(*) as total_rows FROM ({sql_query.rstrip(';')})"
                    count_output = postgres_execute_query(count_query)
                    
                    # Extract count from output (simplified parsing)
                    if "Total rows:" in count_output:
                        lines = count_output.split('\n')
                        for line in lines:
                            if "Total rows:" in line:
                                count_str = line.split("Total rows:")[1].strip()
                                try:
                                    count = int(count_str.replace(',', ''))
                                    output += f"\n\n📊 Total matching rows: {count:,}"
                                    if count > 1000:
                                        output += f"\n⚠️  Showing first 1,000 rows (limited for display)"
                                except:
                                    pass
                                break
            except:
                pass
        
        return output
        
    except Exception as e:
        return f"Error executing query: {e}"

def main():
    """Main function to handle command line usage."""
    parser = argparse.ArgumentParser(description='Convert natural language to SQL for BT Store data')
    parser.add_argument('query', help='Natural language question about the store data')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    
    args = parser.parse_args()
    
    try:
        # Initialize OpenAI client
        openai_client = get_openai_client()
        
        if args.interactive:
            print("🤖 BT Store Analytics - Interactive SQL Generator (PostgreSQL)")
            print("Type 'quit' or 'exit' to stop")
            print("=" * 50)
            
            while True:
                try:
                    query = input("\n💬 Ask a question: ").strip()
                    
                    if query.lower() in ['quit', 'exit', 'q']:
                        print("👋 Goodbye!")
                        break
                    
                    if not query:
                        continue
                    
                    print(f"\n🔍 Processing: {query}")
                    
                    # Generate SQL
                    sql_query = generate_sql_query(query, openai_client)
                    print(f"\n📝 Generated SQL:\n{sql_query}")
                    
                    # Execute query
                    print(f"\n📊 Results:")
                    results = execute_sql_query(sql_query)
                    print(results)
                    
                except KeyboardInterrupt:
                    print("\n👋 Goodbye!")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
        else:
            # Single query mode
            query = args.query
            
            print(f"🔍 Processing: {query}")
            
            # Generate SQL
            sql_query = generate_sql_query(query, openai_client)
            print(f"\n📝 Generated SQL:\n{sql_query}")
            
            # Execute query
            print(f"\n📊 Results:")
            results = execute_sql_query(sql_query)
            print(results)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
