#!/usr/bin/env python3
"""
PostgreSQL Client for BT MIS Analytics
Uses direct PostgreSQL connection for full SQL support
"""

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import json

# Load environment variables (optional for Railway deployment)
load_dotenv(override=False)

class PostgreSQLClient:
    """Client for interacting with PostgreSQL database"""
    
    def __init__(self):
        self.connection_string = os.getenv('DATABASE_URL')
        
        if not self.connection_string:
            raise ValueError("DATABASE_URL must be set in .env file or Railway environment variables")
        
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def execute_query(self, sql_query: str) -> str:
        """Execute SQL query and return results as formatted string"""
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return "Error: Could not connect to database"
            
            # Execute the query
            self.cursor.execute(sql_query)
            
            # Check if it's a SELECT query
            if sql_query.strip().upper().startswith('SELECT'):
                # Fetch results
                results = self.cursor.fetchall()
                columns = [desc[0] for desc in self.cursor.description]
                
                if results:
                    return self._format_results(results, columns)
                else:
                    return "No results found"
            else:
                # For non-SELECT queries, commit the transaction
                self.connection.commit()
                return "Query executed successfully"
                
        except Exception as e:
            return f"Error executing query: {e}"
    
    def _format_results(self, results: List[tuple], columns: List[str]) -> str:
        """Format query results for display"""
        if not results:
            return "No results found"
        
        # Convert to DataFrame for better formatting
        df = pd.DataFrame(results, columns=columns)
        
        # Format the output similar to DuckDB's table format
        output = []
        
        # Add header
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
    
    def create_table(self):
        """Create the mis_long table in PostgreSQL"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS mis_long (
            id SERIAL PRIMARY KEY,
            store_name TEXT,
            parameter TEXT,
            cafe_code TEXT,
            region TEXT,
            category TEXT,
            for_ssg TEXT,
            area_store DOUBLE PRECISION,
            store_start_date DATE,
            vintage TEXT,
            month DATE,
            value DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_mis_long_store_name ON mis_long(store_name);
        CREATE INDEX IF NOT EXISTS idx_mis_long_parameter ON mis_long(parameter);
        CREATE INDEX IF NOT EXISTS idx_mis_long_month ON mis_long(month);
        CREATE INDEX IF NOT EXISTS idx_mis_long_region ON mis_long(region);
        CREATE INDEX IF NOT EXISTS idx_mis_long_category ON mis_long(category);
        """
        
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return False
            
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    
    def get_table_info(self) -> str:
        """Get information about the mis_long table"""
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return "Error: Could not connect to database"
            
            # Get total count
            self.cursor.execute("SELECT COUNT(*) FROM mis_long")
            total_count = self.cursor.fetchone()[0]
            
            # Get sample data
            self.cursor.execute("SELECT * FROM mis_long LIMIT 5")
            sample_results = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            
            info = f"Table: mis_long\n"
            info += f"Total rows: {total_count:,}\n"
            info += f"Sample data:\n"
            
            if sample_results:
                df = pd.DataFrame(sample_results, columns=columns)
                info += df.to_string(index=False)
            
            return info
            
        except Exception as e:
            return f"Error getting table info: {e}"
    
    def test_connection(self) -> bool:
        """Test the connection to PostgreSQL"""
        try:
            if not self.connect():
                return False
            
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            return result[0] == 1
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

# Global instance
postgres_client = None

def get_postgres_client() -> PostgreSQLClient:
    """Get or create the global PostgreSQL client instance"""
    global postgres_client
    if postgres_client is None:
        postgres_client = PostgreSQLClient()
    return postgres_client

def execute_sql_query(sql_query: str) -> str:
    """Execute SQL query using PostgreSQL (replaces DuckDB function)"""
    client = get_postgres_client()
    return client.execute_query(sql_query)

if __name__ == "__main__":
    # Test the client
    client = PostgreSQLClient()
    
    if client.test_connection():
        print("✅ PostgreSQL connection successful!")
        print("\n📊 Table info:")
        print(client.get_table_info())
    else:
        print("❌ PostgreSQL connection failed!")
        print("💡 Make sure your .env file has correct DATABASE_URL")
