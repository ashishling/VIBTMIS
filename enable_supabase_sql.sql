-- Enable raw SQL execution in Supabase
-- Run this in Supabase SQL Editor to enable exec_sql function

-- Create a function to execute arbitrary SQL
CREATE OR REPLACE FUNCTION exec_sql(sql text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result json;
BEGIN
    -- Execute the SQL and return as JSON
    EXECUTE 'SELECT json_agg(row_to_json(t)) FROM (' || sql || ') t' INTO result;
    RETURN COALESCE(result, '[]'::json);
END;
$$;

-- Grant execute permission to authenticated users
GRANT EXECUTE ON FUNCTION exec_sql(text) TO authenticated;

-- Test the function
SELECT exec_sql('SELECT store_name, SUM(value) as total_revenue FROM mis_long WHERE parameter = ''Revenue'' GROUP BY store_name ORDER BY total_revenue DESC LIMIT 5');
