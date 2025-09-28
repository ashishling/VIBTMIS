#!/usr/bin/env python3
"""
Neon PostgreSQL Migration Script for BT MIS Analytics
Migrates data from Supabase to Neon PostgreSQL
"""

import pandas as pd
import os
from dotenv import load_dotenv
from postgres_client import PostgreSQLClient
import time

# Load environment variables
load_dotenv()

class NeonMigrator:
    def __init__(self):
        self.postgres_client = PostgreSQLClient()
    
    def create_table_schema(self):
        """Create the mis_long table in Neon PostgreSQL"""
        print("📋 Creating table schema in Neon PostgreSQL...")
        
        if self.postgres_client.create_table():
            print("✅ Table schema created successfully")
            return True
        else:
            print("❌ Error creating table schema")
            return False
    
    def load_csv_data(self, csv_path: str = 'clean_mis_long.csv'):
        """Load and prepare CSV data for migration"""
        print(f"📊 Loading data from {csv_path}...")
        
        try:
            df = pd.read_csv(csv_path)
            print(f"✅ Loaded {len(df)} rows from CSV")
            
            # Clean and prepare data
            df = df.dropna(subset=['value'])  # Remove rows with null values
            
            # Handle date columns
            df['store_start_date'] = pd.to_datetime(df['store_start_date'], errors='coerce')
            df['month'] = pd.to_datetime(df['month'], errors='coerce')
            
            # Clean numeric columns - replace NaN with None (NULL in database)
            numeric_columns = ['area_store', 'value']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].replace([float('nan'), float('inf'), float('-inf')], None)
            
            # Convert dates to ISO format strings for PostgreSQL
            date_columns = ['store_start_date', 'month']
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].dt.strftime('%Y-%m-%d').replace('NaT', None)
            
            # Remove any remaining rows with NaN values
            df = df.dropna()
            
            print(f"✅ Prepared {len(df)} records for migration")
            return df
            
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return None
    
    def migrate_data(self, df: pd.DataFrame, batch_size: int = 1000):
        """Migrate data to Neon PostgreSQL in batches"""
        print(f"🚀 Starting migration of {len(df)} records...")
        
        if not self.postgres_client.connect():
            print("❌ Could not connect to PostgreSQL")
            return 0
        
        total_batches = (len(df) + batch_size - 1) // batch_size
        successful_records = 0
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_df)} records)...")
            
            try:
                # Use bulk INSERT with VALUES for PostgreSQL
                successful_in_batch = self._insert_batch_values(batch_df, batch_num)
                successful_records += successful_in_batch
                
                if successful_in_batch > 0:
                    print(f"✅ Batch {batch_num} inserted successfully ({successful_in_batch} records)")
                
                # Small delay to avoid overwhelming the database
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error inserting batch {batch_num}: {e}")
                # Try individual inserts for this batch
                individual_success = self._insert_individual_records(batch_df, batch_num)
                successful_records += individual_success
                continue
        
        self.postgres_client.disconnect()
        print(f"🎉 Migration completed! {successful_records}/{len(df)} records migrated successfully")
        return successful_records
    
    def _insert_batch_values(self, batch_df: pd.DataFrame, batch_num: int) -> int:
        """Insert batch using PostgreSQL VALUES syntax"""
        try:
            # Get column names (excluding id since it's auto-generated)
            columns = [col for col in batch_df.columns if col != 'id']
            columns_str = ', '.join(columns)
            
            # Create VALUES clause
            values_list = []
            for _, row in batch_df.iterrows():
                # Handle NULL values and escape strings
                row_values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_values.append('NULL')
                    elif isinstance(value, str):
                        # Escape single quotes in strings
                        escaped_value = value.replace("'", "''")
                        row_values.append(f"'{escaped_value}'")
                    else:
                        row_values.append(str(value))
                
                values_list.append(f"({', '.join(row_values)})")
            
            # Create the INSERT statement
            values_str = ', '.join(values_list)
            insert_sql = f"INSERT INTO mis_long ({columns_str}) VALUES {values_str}"
            
            # Execute the batch insert
            self.postgres_client.cursor.execute(insert_sql)
            self.postgres_client.connection.commit()
            
            return len(batch_df)
            
        except Exception as e:
            print(f"⚠️  Batch VALUES insert failed: {e}")
            # Fallback to individual inserts
            return self._insert_individual_records(batch_df, batch_num)
    
    def _insert_individual_records(self, batch_df: pd.DataFrame, batch_num: int) -> int:
        """Insert records one by one to identify problematic ones"""
        successful = 0
        
        for i, (_, row) in enumerate(batch_df.iterrows()):
            try:
                # Create INSERT statement
                columns = ', '.join(row.index)
                placeholders = ', '.join(['%s'] * len(row))
                insert_sql = f"INSERT INTO mis_long ({columns}) VALUES ({placeholders})"
                
                self.postgres_client.cursor.execute(insert_sql, tuple(row.values))
                successful += 1
            except Exception as e:
                print(f"⚠️  Record {i+1} in batch {batch_num} failed: {e}")
                continue
        
        return successful
    
    def verify_migration(self):
        """Verify the migration was successful"""
        print("🔍 Verifying migration...")
        
        try:
            if not self.postgres_client.connect():
                return False
            
            info = self.postgres_client.get_table_info()
            print("✅ Verification successful!")
            print(info)
            
            self.postgres_client.disconnect()
            return True
            
        except Exception as e:
            print(f"❌ Verification failed: {e}")
            return False
    
    def run_migration(self, csv_path: str = 'clean_mis_long.csv'):
        """Run the complete migration process"""
        print("🚀 Starting Neon PostgreSQL Migration for BT MIS Analytics")
        print("=" * 60)
        
        # Step 1: Create table schema
        if not self.create_table_schema():
            return False
        
        # Step 2: Load CSV data
        df = self.load_csv_data(csv_path)
        if df is None:
            return False
        
        # Step 3: Migrate data
        successful_records = self.migrate_data(df)
        if successful_records == 0:
            return False
        
        # Step 4: Verify migration
        if not self.verify_migration():
            return False
        
        print("=" * 60)
        print("🎉 Migration completed successfully!")
        print(f"📊 {successful_records} records migrated to Neon PostgreSQL")
        print("🔗 Your data is now ready for the cloud!")
        
        return True

def main():
    """Main function to run the migration"""
    try:
        migrator = NeonMigrator()
        success = migrator.run_migration()
        
        if success:
            print("\n✅ Next steps:")
            print("1. Test PostgreSQL connection: python postgres_client.py")
            print("2. Test SQL queries: python nl_to_sql_postgres.py")
            print("3. Update Flask app: python app.py")
        else:
            print("\n❌ Migration failed. Please check the errors above.")
            
    except Exception as e:
        print(f"❌ Migration failed with error: {e}")
        print("\n💡 Make sure you have:")
        print("1. Created a Neon PostgreSQL project")
        print("2. Added DATABASE_URL to your .env file")
        print("3. Installed psycopg2-binary: pip install psycopg2-binary")

if __name__ == "__main__":
    main()
