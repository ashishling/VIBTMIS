# BTC Store Data Processing

This project contains tools to process BTC store data from cross-tabulated CSV format to tidy long format for analysis with DuckDB or SQLite.

## Files

- `process_btc_csv.py` - Main processing script
- `clean_mis.py` - Original Excel processing script (legacy)
- `BTC store for CSV.csv` - Input data file
- `clean_mis_long.csv` - Output tidy long format data
- `duckdb_load.sql` - DuckDB table creation and loading script
- `requirements.txt` - Python dependencies

## Setup

1. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up OpenAI API key (for natural language queries):
```bash
export OPENAI_API_KEY='your-openai-api-key-here'
```

4. (Optional) Set up Local LLM for AI summaries:
```bash
# Install Ollama
brew install ollama

# Start Ollama
ollama serve

# Download a model
ollama pull llama3.2
```

## Usage

### 1. Data Processing

Process the CSV file to tidy long format:

```bash
python process_btc_csv.py --input "BTC store for CSV.csv" --output "clean_mis_long.csv" --verbose
```

### 2. Natural Language Queries (NEW!)

Ask questions about your data in plain English:

**Single Query:**
```bash
python nl_to_sql.py "What are the top 10 stores by revenue in 2024?"
python nl_to_sql.py "Show me monthly transaction trends"
python nl_to_sql.py "Which region has the highest EBITDA?"
```

**Interactive Mode:**
```bash
python interactive_query.py
```

**Web UI (NEW!):**
```bash
python app.py
```
Then open your browser and go to: http://localhost:8080

**Examples of natural language queries:**
- "What are the top performing stores by revenue?"
- "Show me the average store area by region"
- "Which category has higher transaction volume?"
- "What's the revenue per square foot for each store?"
- "Show me monthly trends for EBITDA"
- "Which stores have the highest profit margins?"

**With Local LLM Summaries:**
- Ask any question and click "🤖 Summarize Results" for AI-powered insights
- Get business summaries with key findings and recommendations
- All processing happens locally - your data never leaves your machine

### Command Line Options

- `--input, -i`: Path to input CSV file (required)
- `--output, -o`: Path to output CSV file (required)  
- `--verbose, -v`: Enable verbose output (optional)

## Data Processing

The script performs the following transformations:

1. **Header Detection**: Automatically finds the header row in the CSV
2. **Column Renaming**: Converts key columns to snake_case:
   - `Store Name` → `store_name`
   - `Parameter` → `parameter`
   - `Cafe Codes` → `cafe_code`
   - `Region` → `region`
   - `Category` → `category`
   - `FOR SSG` → `for_ssg`
   - `Area` → `area_store`
   - `Store Start Date` → `store_start_date`
   - `Vintage` → `vintage`

3. **Month Column Detection**: Identifies month columns (Apr-21, May-21, etc.) and converts them to first-of-month timestamps

4. **Data Melting**: Transforms from wide to long format with one row per (store, parameter, month, value)

5. **Value Cleaning**:
   - Removes commas from numeric values
   - Handles percentage values (converts to decimal 0-1)
   - Converts empty/null values appropriately
   - Handles parentheses for negative numbers

## Output

The script generates two files:

1. **`clean_mis_long.csv`**: Tidy long format data with columns:
   - `store_name`: Store identifier
   - `parameter`: Metric type (Revenue, EBITDA, Transactions, etc.)
   - `cafe_code`: Store code
   - `region`: Geographic region
   - `category`: Store category
   - `for_ssg`: SSG flag
   - `area_store`: Store area in sq ft
   - `store_start_date`: Store opening date
   - `vintage`: Store vintage
   - `month`: Month (first of month)
   - `value`: Numeric value

2. **`duckdb_load.sql`**: SQL script to create and load data into DuckDB

## DuckDB Usage

Load the data into DuckDB:

```bash
duckdb
.read duckdb_load.sql
```

Example queries:

```sql
-- Revenue by region for 2024
SELECT 
    region,
    SUM(value) as total_revenue
FROM mis_long 
WHERE parameter = 'Revenue' 
  AND month BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY region 
ORDER BY total_revenue DESC;

-- Store performance comparison
SELECT 
    store_name,
    region,
    category,
    SUM(CASE WHEN parameter = 'Revenue' THEN value ELSE 0 END) as revenue,
    SUM(CASE WHEN parameter = 'EBITDA' THEN value ELSE 0 END) as ebitda
FROM mis_long 
WHERE month >= '2024-01-01'
GROUP BY store_name, region, category
ORDER BY revenue DESC;
```

## Data Summary

The processed dataset contains:
- **154,440 rows** of data
- **198 unique stores**
- **14 different parameters** (Area, Revenue, EBITDA, Transactions, etc.)
- **Date range**: April 2021 to July 2025
- **52 months** of data per store/parameter combination
