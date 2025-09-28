#!/usr/bin/env python3
"""
Supabase Migration Script for BT MIS Analytics
Migrates data from local CSV to Supabase PostgreSQL database
"""

import pandas as pd
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import time
from typing import Optional

# Load environment variables
load_dotenv()

class SupabaseMigrator:
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    def create_table_schema(self):
        """Create the mis_long table in Supabase"""
        print("📋 Creating table schema...")
        print("💡 Note: You'll need to create the table manually in Supabase dashboard")
        print("   Go to: Table Editor → New Table")
        print("   Table name: mis_long")
        print("   Columns:")
        print("   - id (int8, primary key, auto-increment)")
        print("   - store_name (text)")
        print("   - parameter (text)")
        print("   - cafe_code (text)")
        print("   - region (text)")
        print("   - category (text)")
        print("   - for_ssg (text)")
        print("   - area_store (float8)")
        print("   - store_start_date (date)")
        print("   - vintage (text)")
        print("   - month (date)")
        print("   - value (float8)")
        print("   - created_at (timestamptz, default: now())")
        print()
        
        # Check if table exists by trying to query it
        try:
            result = self.supabase.table('mis_long').select('id').limit(1).execute()
            print("✅ Table 'mis_long' already exists and is accessible")
            return True
        except Exception as e:
            print(f"❌ Table 'mis_long' not found or not accessible: {e}")
            print("📋 Please create the table manually in Supabase dashboard first")
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
            
            # Convert dates to ISO format strings for JSON serialization
            date_columns = ['store_start_date', 'month']
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].dt.strftime('%Y-%m-%d').replace('NaT', None)
            
            # Remove any remaining rows with NaN values
            df = df.dropna()
            
            # Convert to dict for Supabase
            data = df.to_dict('records')
            print(f"✅ Prepared {len(data)} records for migration")
            
            return data
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return None
    
    def migrate_data(self, data: list, batch_size: int = 1000):
        """Migrate data to Supabase in batches"""
        print(f"🚀 Starting migration of {len(data)} records...")
        
        total_batches = (len(data) + batch_size - 1) // batch_size
        successful_records = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")
            
            try:
                # Clean the batch data before sending
                cleaned_batch = self._clean_batch_data(batch)
                
                if not cleaned_batch:
                    print(f"⚠️  Batch {batch_num} skipped (no valid records after cleaning)")
                    continue
                
                # Insert batch into Supabase
                result = self.supabase.table('mis_long').insert(cleaned_batch).execute()
                successful_records += len(cleaned_batch)
                print(f"✅ Batch {batch_num} inserted successfully ({len(cleaned_batch)} records)")
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error inserting batch {batch_num}: {e}")
                # Try to insert records one by one to identify problematic records
                print(f"🔍 Attempting individual record insertion for batch {batch_num}...")
                individual_success = self._insert_individual_records(batch, batch_num)
                successful_records += individual_success
                continue
        
        print(f"🎉 Migration completed! {successful_records}/{len(data)} records migrated successfully")
        return successful_records
    
    def _clean_batch_data(self, batch: list) -> list:
        """Clean batch data to ensure JSON compatibility"""
        cleaned_batch = []
        
        for record in batch:
            try:
                # Create a clean copy of the record
                clean_record = {}
                
                for key, value in record.items():
                    # Handle different data types
                    if pd.isna(value) or value is None:
                        clean_record[key] = None
                    elif isinstance(value, (int, float)):
                        # Check for NaN, inf, -inf
                        if pd.isna(value) or value in [float('inf'), float('-inf')]:
                            clean_record[key] = None
                        else:
                            clean_record[key] = value
                    elif isinstance(value, str):
                        clean_record[key] = value
                    else:
                        # Convert other types to string
                        clean_record[key] = str(value)
                
                cleaned_batch.append(clean_record)
                
            except Exception as e:
                print(f"⚠️  Skipping problematic record: {e}")
                continue
        
        return cleaned_batch
    
    def _insert_individual_records(self, batch: list, batch_num: int) -> int:
        """Insert records one by one to identify problematic ones"""
        successful = 0
        
        for i, record in enumerate(batch):
            try:
                cleaned_record = self._clean_batch_data([record])[0]
                result = self.supabase.table('mis_long').insert([cleaned_record]).execute()
                successful += 1
            except Exception as e:
                print(f"⚠️  Record {i+1} in batch {batch_num} failed: {e}")
                continue
        
        return successful
    
    def verify_migration(self):
        """Verify the migration was successful"""
        print("🔍 Verifying migration...")
        
        try:
            # Get total count
            result = self.supabase.table('mis_long').select('id', count='exact').execute()
            total_count = result.count
            
            # Get sample data
            sample = self.supabase.table('mis_long').select('*').limit(5).execute()
            
            print(f"✅ Verification successful!")
            print(f"📊 Total records in database: {total_count}")
            print(f"📋 Sample records:")
            for record in sample.data:
                print(f"   - {record['store_name']} | {record['parameter']} | {record['month']} | {record['value']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Verification failed: {e}")
            return False
    
    def run_migration(self, csv_path: str = 'clean_mis_long.csv'):
        """Run the complete migration process"""
        print("🚀 Starting Supabase Migration for BT MIS Analytics")
        print("=" * 50)
        
        # Step 1: Create table schema
        if not self.create_table_schema():
            return False
        
        # Step 2: Load CSV data
        data = self.load_csv_data(csv_path)
        if not data:
            return False
        
        # Step 3: Migrate data
        successful_records = self.migrate_data(data)
        if successful_records == 0:
            return False
        
        # Step 4: Verify migration
        if not self.verify_migration():
            return False
        
        print("=" * 50)
        print("🎉 Migration completed successfully!")
        print(f"📊 {successful_records} records migrated to Supabase")
        print("🔗 Your data is now ready for the cloud!")
        
        return True

def main():
    """Main function to run the migration"""
    try:
        migrator = SupabaseMigrator()
        success = migrator.run_migration()
        
        if success:
            print("\n✅ Next steps:")
            print("1. Update your .env file with Supabase credentials")
            print("2. Run: python update_app_for_supabase.py")
            print("3. Test locally: python app.py")
        else:
            print("\n❌ Migration failed. Please check the errors above.")
            
    except Exception as e:
        print(f"❌ Migration failed with error: {e}")
        print("\n💡 Make sure you have:")
        print("1. Created a Supabase project")
        print("2. Added SUPABASE_URL and SUPABASE_ANON_KEY to your .env file")
        print("3. Installed supabase-py: pip install supabase")

if __name__ == "__main__":
    main()
