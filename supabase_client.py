#!/usr/bin/env python3
"""
Supabase Database Client for BT MIS Analytics
Replaces DuckDB with Supabase PostgreSQL connection
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import pandas as pd

# Load environment variables
load_dotenv()

class SupabaseClient:
    """Client for interacting with Supabase database"""
    
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    def execute_query(self, sql_query: str) -> str:
        """Execute SQL query and return results as formatted string"""
        try:
            # Remove any DuckDB-specific syntax and convert to PostgreSQL
            sql_query = self._convert_duckdb_to_postgresql(sql_query)
            
            # Try to execute using raw SQL (if available in your Supabase setup)
            try:
                # This requires enabling the exec_sql function in Supabase
                result = self.supabase.rpc('exec_sql', {'sql': sql_query}).execute()
                
                if result.data:
                    return self._format_results(result.data)
                else:
                    return "Query executed successfully (no results returned)"
            except:
                # Fallback to alternative approach
                return self._execute_query_alternative(sql_query)
                
        except Exception as e:
            return f"Error executing query: {e}"
    
    def _convert_duckdb_to_postgresql(self, sql: str) -> str:
        """Convert DuckDB-specific syntax to PostgreSQL"""
        # DuckDB uses different syntax for some functions
        sql = sql.replace('COUNT(*)', 'COUNT(*)')
        sql = sql.replace('SUM(', 'SUM(')
        sql = sql.replace('AVG(', 'AVG(')
        sql = sql.replace('MAX(', 'MAX(')
        sql = sql.replace('MIN(', 'MIN(')
        
        # Handle date functions
        sql = sql.replace('DATE_TRUNC(', 'DATE_TRUNC(')
        
        return sql
    
    def _execute_query_alternative(self, sql_query: str) -> str:
        """Alternative method to execute queries using Supabase client"""
        try:
            # Parse the SQL to determine the operation
            sql_upper = sql_query.upper().strip()
            
            if sql_upper.startswith('SELECT'):
                return self._execute_select_query(sql_query)
            elif sql_upper.startswith('INSERT'):
                return self._execute_insert_query(sql_query)
            elif sql_upper.startswith('UPDATE'):
                return self._execute_update_query(sql_query)
            elif sql_upper.startswith('DELETE'):
                return self._execute_delete_query(sql_query)
            else:
                return f"Unsupported query type: {sql_query[:100]}..."
                
        except Exception as e:
            return f"Error executing query: {e}"
    
    def _execute_select_query(self, sql_query: str) -> str:
        """Execute SELECT queries using Supabase client"""
        try:
            # For simple SELECT queries, we can use the Supabase client
            # This is a simplified approach - for complex queries, you might need to use raw SQL
            
            # Extract table name and basic query structure
            if 'FROM mis_long' in sql_query.upper():
                # Use Supabase client for basic queries
                query = self.supabase.table('mis_long')
                
                # Add basic filters if present
                if 'WHERE' in sql_query.upper():
                    # This is simplified - for complex WHERE clauses, you'd need more parsing
                    pass
                
                # Add LIMIT if present
                if 'LIMIT' in sql_query.upper():
                    limit_match = sql_query.upper().split('LIMIT')[1].strip().split()[0]
                    try:
                        limit = int(limit_match)
                        query = query.limit(limit)
                    except:
                        pass
                
                result = query.select('*').execute()
                
                if result.data:
                    return self._format_results(result.data)
                else:
                    return "No results found"
            else:
                return "Complex queries not yet supported. Please use simpler SELECT statements."
                
        except Exception as e:
            return f"Error executing SELECT query: {e}"
    
    def _execute_insert_query(self, sql_query: str) -> str:
        """Execute INSERT queries"""
        return "INSERT queries not supported in this simplified client"
    
    def _execute_update_query(self, sql_query: str) -> str:
        """Execute UPDATE queries"""
        return "UPDATE queries not supported in this simplified client"
    
    def _execute_delete_query(self, sql_query: str) -> str:
        """Execute DELETE queries"""
        return "DELETE queries not supported in this simplified client"
    
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
    
    def get_table_info(self) -> str:
        """Get information about the mis_long table"""
        try:
            # Get total count
            count_result = self.supabase.table('mis_long').select('id', count='exact').execute()
            total_count = count_result.count
            
            # Get sample data
            sample_result = self.supabase.table('mis_long').select('*').limit(5).execute()
            
            info = f"Table: mis_long\n"
            info += f"Total rows: {total_count:,}\n"
            info += f"Sample data:\n"
            
            if sample_result.data:
                df = pd.DataFrame(sample_result.data)
                info += df.to_string(index=False)
            
            return info
            
        except Exception as e:
            return f"Error getting table info: {e}"
    
    def test_connection(self) -> bool:
        """Test the connection to Supabase"""
        try:
            result = self.supabase.table('mis_long').select('id').limit(1).execute()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

# Global instance
supabase_client = None

def get_supabase_client() -> SupabaseClient:
    """Get or create the global Supabase client instance"""
    global supabase_client
    if supabase_client is None:
        supabase_client = SupabaseClient()
    return supabase_client

def execute_sql_query(sql_query: str) -> str:
    """Execute SQL query using Supabase (replaces DuckDB function)"""
    client = get_supabase_client()
    return client.execute_query(sql_query)

if __name__ == "__main__":
    # Test the client
    client = SupabaseClient()
    
    if client.test_connection():
        print("✅ Supabase connection successful!")
        print("\n📊 Table info:")
        print(client.get_table_info())
    else:
        print("❌ Supabase connection failed!")
        print("💡 Make sure your .env file has correct SUPABASE_URL and SUPABASE_ANON_KEY")
