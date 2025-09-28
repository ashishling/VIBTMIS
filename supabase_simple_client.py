#!/usr/bin/env python3
"""
Simple Supabase Client for BT MIS Analytics
Uses Supabase's built-in query capabilities for common operations
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import pandas as pd

# Load environment variables
load_dotenv()

class SimpleSupabaseClient:
    """Simple client for common Supabase operations"""
    
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    def execute_simple_query(self, sql_query: str) -> str:
        """Execute simple queries using Supabase client methods"""
        try:
            sql_upper = sql_query.upper().strip()
            
            # Handle different types of queries
            if 'SELECT' in sql_upper and 'FROM mis_long' in sql_upper:
                return self._handle_select_query(sql_query)
            else:
                return "Complex queries not yet supported. Please use simpler SELECT statements."
                
        except Exception as e:
            return f"Error executing query: {e}"
    
    def _handle_select_query(self, sql_query: str) -> str:
        """Handle SELECT queries using Supabase client"""
        try:
            # Parse the query to extract basic components
            sql_upper = sql_query.upper()
            
            # Start with base query
            query = self.supabase.table('mis_long')
            
            # Handle WHERE clauses
            if 'WHERE' in sql_upper:
                where_clause = self._extract_where_clause(sql_query)
                if where_clause:
                    query = self._apply_where_clause(query, where_clause)
            
            # Handle GROUP BY
            if 'GROUP BY' in sql_upper:
                group_by = self._extract_group_by(sql_query)
                if group_by:
                    query = query.select(f"{group_by}, SUM(value) as total_value")
                else:
                    query = query.select('*')
            else:
                query = query.select('*')
            
            # Handle ORDER BY
            if 'ORDER BY' in sql_upper:
                order_by = self._extract_order_by(sql_query)
                if order_by:
                    if 'DESC' in sql_upper:
                        query = query.order(order_by, desc=True)
                    else:
                        query = query.order(order_by)
            
            # Handle LIMIT
            if 'LIMIT' in sql_upper:
                limit = self._extract_limit(sql_query)
                if limit:
                    query = query.limit(limit)
            else:
                query = query.limit(1000)  # Default limit
            
            # Execute the query
            result = query.execute()
            
            if result.data:
                return self._format_results(result.data)
            else:
                return "No results found"
                
        except Exception as e:
            return f"Error executing SELECT query: {e}"
    
    def _extract_where_clause(self, sql_query: str) -> Optional[str]:
        """Extract WHERE clause from SQL"""
        try:
            sql_upper = sql_query.upper()
            where_start = sql_upper.find('WHERE')
            if where_start == -1:
                return None
            
            # Find the end of WHERE clause
            where_end = sql_upper.find('GROUP BY', where_start)
            if where_end == -1:
                where_end = sql_upper.find('ORDER BY', where_start)
            if where_end == -1:
                where_end = sql_upper.find('LIMIT', where_start)
            if where_end == -1:
                where_end = len(sql_query)
            
            where_clause = sql_query[where_start + 5:where_end].strip()
            return where_clause
        except:
            return None
    
    def _extract_group_by(self, sql_query: str) -> Optional[str]:
        """Extract GROUP BY clause from SQL"""
        try:
            sql_upper = sql_query.upper()
            group_start = sql_upper.find('GROUP BY')
            if group_start == -1:
                return None
            
            group_end = sql_upper.find('ORDER BY', group_start)
            if group_end == -1:
                group_end = sql_upper.find('LIMIT', group_start)
            if group_end == -1:
                group_end = len(sql_query)
            
            group_clause = sql_query[group_start + 8:group_end].strip()
            return group_clause
        except:
            return None
    
    def _extract_order_by(self, sql_query: str) -> Optional[str]:
        """Extract ORDER BY clause from SQL"""
        try:
            sql_upper = sql_query.upper()
            order_start = sql_upper.find('ORDER BY')
            if order_start == -1:
                return None
            
            order_end = sql_upper.find('LIMIT', order_start)
            if order_end == -1:
                order_end = len(sql_query)
            
            order_clause = sql_query[order_start + 8:order_end].strip()
            # Remove DESC/ASC
            order_clause = order_clause.replace('DESC', '').replace('ASC', '').strip()
            return order_clause
        except:
            return None
    
    def _extract_limit(self, sql_query: str) -> Optional[int]:
        """Extract LIMIT clause from SQL"""
        try:
            sql_upper = sql_query.upper()
            limit_start = sql_upper.find('LIMIT')
            if limit_start == -1:
                return None
            
            limit_clause = sql_query[limit_start + 5:].strip()
            limit_value = int(limit_clause.split()[0])
            return limit_value
        except:
            return None
    
    def _apply_where_clause(self, query, where_clause: str):
        """Apply WHERE clause to Supabase query"""
        try:
            # Simple WHERE clause handling
            if "parameter = 'Revenue'" in where_clause:
                query = query.eq('parameter', 'Revenue')
            elif "parameter = 'EBITDA'" in where_clause:
                query = query.eq('parameter', 'EBITDA')
            elif "parameter = 'Transactions'" in where_clause:
                query = query.eq('parameter', 'Transactions')
            elif "parameter = 'Area'" in where_clause:
                query = query.eq('parameter', 'Area')
            
            # Add more WHERE clause handling as needed
            return query
        except:
            return query
    
    def _format_results(self, data: List[Dict[str, Any]]) -> str:
        """Format query results for display"""
        if not data:
            return "No results found"
        
        # Convert to DataFrame for better formatting
        df = pd.DataFrame(data)
        
        # Format the output similar to DuckDB's table format
        output = []
        
        # Add header
        columns = list(df.columns)
        header = " | ".join(f"{col:>15}" for col in columns)
        output.append(header)
        output.append("-" * len(header))
        
        # Add rows (limit to 1000 for display)
        for _, row in df.head(1000).iterrows():
            row_str = " | ".join(f"{str(val):>15}" for val in row.values)
            output.append(row_str)
        
        # Add summary
        if len(df) > 1000:
            output.append(f"\n... and {len(df) - 1000} more rows")
        
        output.append(f"\nTotal rows: {len(df)}")
        
        return "\n".join(output)
    
    def test_connection(self) -> bool:
        """Test the connection to Supabase"""
        try:
            result = self.supabase.table('mis_long').select('id').limit(1).execute()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

# Global instance
simple_supabase_client = None

def get_simple_supabase_client() -> SimpleSupabaseClient:
    """Get or create the global simple Supabase client instance"""
    global simple_supabase_client
    if simple_supabase_client is None:
        simple_supabase_client = SimpleSupabaseClient()
    return simple_supabase_client

def execute_sql_query(sql_query: str) -> str:
    """Execute SQL query using simple Supabase client"""
    client = get_simple_supabase_client()
    return client.execute_simple_query(sql_query)

if __name__ == "__main__":
    # Test the client
    client = SimpleSupabaseClient()
    
    if client.test_connection():
        print("✅ Supabase connection successful!")
        
        # Test a simple query
        test_query = "SELECT store_name, SUM(value) as total_revenue FROM mis_long WHERE parameter = 'Revenue' GROUP BY store_name ORDER BY total_revenue DESC LIMIT 5"
        print(f"\n🔍 Testing query: {test_query}")
        result = client.execute_simple_query(test_query)
        print(f"\n📊 Results:\n{result}")
    else:
        print("❌ Supabase connection failed!")
        print("💡 Make sure your .env file has correct SUPABASE_URL and SUPABASE_ANON_KEY")
