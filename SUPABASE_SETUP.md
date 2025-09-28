# Supabase Setup Guide

## Step 1: Create Supabase Project

1. Go to [supabase.com](https://supabase.com) and sign in
2. Click "New Project"
3. Choose your organization
4. Enter project details:
   - **Name**: `bt-mis-analytics`
   - **Database Password**: Choose a strong password
   - **Region**: Choose closest to your users
5. Click "Create new project"
6. Wait for project to be ready (2-3 minutes)

## Step 2: Get Your Credentials

1. In your Supabase dashboard, go to **Settings** → **API**
2. Copy the following values:
   - **Project URL** (looks like: `https://abcdefgh.supabase.co`)
   - **anon public key** (starts with `eyJ...`)

## Step 3: Update Your .env File

Add these lines to your `.env` file:

```bash
# Supabase Configuration
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your_supabase_anon_key_here
```

## Step 4: Install Dependencies

```bash
source venv/bin/activate
pip install supabase
```

## Step 5: Run Migration

```bash
python supabase_migration.py
```

## Step 6: Verify Data

1. Go to your Supabase dashboard
2. Click **Table Editor**
3. You should see the `mis_long` table with your data
4. Check that row count matches your CSV file

## Troubleshooting

### Common Issues:

1. **"Invalid API key"**: Double-check your SUPABASE_ANON_KEY
2. **"Project not found"**: Verify your SUPABASE_URL
3. **"Permission denied"**: Make sure you're using the anon key, not service role key
4. **"Table already exists"**: The migration script will handle this automatically

### Getting Help:

- Check Supabase logs in the dashboard
- Verify your .env file has the correct values
- Make sure your CSV file exists in the project directory
