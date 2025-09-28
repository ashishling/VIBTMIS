-- Create the mis_long table in Supabase
-- Run this in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS mis_long (
    id BIGSERIAL PRIMARY KEY,
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_mis_long_store_name ON mis_long(store_name);
CREATE INDEX IF NOT EXISTS idx_mis_long_parameter ON mis_long(parameter);
CREATE INDEX IF NOT EXISTS idx_mis_long_month ON mis_long(month);
CREATE INDEX IF NOT EXISTS idx_mis_long_region ON mis_long(region);
CREATE INDEX IF NOT EXISTS idx_mis_long_category ON mis_long(category);

-- Enable Row Level Security (optional, for better security)
ALTER TABLE mis_long ENABLE ROW LEVEL SECURITY;

-- Create a policy that allows all operations (adjust as needed)
CREATE POLICY "Allow all operations on mis_long" ON mis_long
    FOR ALL USING (true);

-- Verify table creation
SELECT 'Table created successfully' as status;
