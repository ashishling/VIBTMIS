# Neon PostgreSQL Setup Guide

## Step 1: Get Your Connection String

1. Go to your Neon dashboard
2. Click on your project
3. Go to **Connection Details** or **Settings**
4. Copy the **Connection String** (it looks like: `postgresql://username:password@hostname/database?sslmode=require`)

## Step 2: Update Your .env File

Add this line to your `.env` file:

```bash
# Neon PostgreSQL Configuration
DATABASE_URL=postgresql://username:password@hostname/database?sslmode=require
```

**Important**: Replace the connection string with your actual Neon connection string.

## Step 3: Install Dependencies

```bash
source venv/bin/activate
pip install psycopg2-binary
```

## Step 4: Test Connection

```bash
python postgres_client.py
```

You should see:
```
✅ PostgreSQL connection successful!
```

## Step 5: Run Migration

```bash
python neon_migration.py
```

This will:
- Create the `mis_long` table
- Migrate your data from CSV to Neon PostgreSQL
- Verify the migration

## Step 6: Test SQL Queries

```bash
python nl_to_sql_postgres.py "What are the top 5 stores by revenue?"
```

## Step 7: Update Flask App

Update your Flask app to use PostgreSQL:

```bash
# In app.py, change the import from:
from nl_to_sql_supabase import get_openai_client, generate_sql_query, execute_sql_query

# To:
from nl_to_sql_postgres import get_openai_client, generate_sql_query, execute_sql_query
```

## Troubleshooting

### Common Issues:

1. **"Connection refused"**: Check your DATABASE_URL format
2. **"SSL required"**: Make sure your connection string includes `?sslmode=require`
3. **"Authentication failed"**: Verify your username and password
4. **"Database not found"**: Check your database name in the connection string

### Getting Help:

- Check Neon dashboard for connection details
- Verify your .env file has the correct DATABASE_URL
- Make sure your CSV file exists in the project directory
- Check Neon logs in the dashboard

## Benefits of Neon PostgreSQL:

- ✅ **Full SQL support** - All PostgreSQL features work
- ✅ **Free tier** - 3GB storage, generous limits
- ✅ **Serverless** - Scales automatically
- ✅ **Fast queries** - Optimized for performance
- ✅ **No API limitations** - Direct PostgreSQL connection
