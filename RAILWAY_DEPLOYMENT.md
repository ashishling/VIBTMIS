# Railway Deployment Guide

## Step 1: Prepare Your Code

Your code is already ready! The following files are configured:
- ✅ `Procfile` - Tells Railway how to start your app
- ✅ `railway.json` - Railway configuration
- ✅ `requirements.txt` - Python dependencies
- ✅ All your Python files are ready

## Step 2: Railway Website Setup

### 2.1: Create Railway Account
1. Go to [railway.app](https://railway.app)
2. Click "Login" and sign up with GitHub (recommended)
3. Verify your email if prompted

### 2.2: Create New Project
1. Click "New Project"
2. Select "Deploy from GitHub repo"
3. Choose your repository (BTCDuckDB)
4. Click "Deploy"

### 2.3: Set Environment Variables
1. In your Railway project dashboard, click on your service
2. Go to "Variables" tab
3. Add these environment variables:

```
OPENAI_API_KEY=your_actual_openai_api_key
DATABASE_URL=your_neon_postgresql_connection_string
FLASK_ENV=production
FLASK_DEBUG=False
```

**Important**: Replace the values with your actual keys!

### 2.4: Configure Port
Railway automatically sets the PORT environment variable, but let's make sure your app uses it:

Your `app.py` already has the correct configuration:
```python
app.run(debug=True, host='0.0.0.0', port=8080)
```

Railway will override the port automatically.

## Step 3: Deploy

### 3.1: Push to GitHub
Make sure all your files are committed and pushed to GitHub:
```bash
git add .
git commit -m "Add Railway deployment configuration"
git push origin main
```

### 3.2: Railway Auto-Deploy
Railway will automatically:
1. Detect your Python app
2. Install dependencies from `requirements.txt`
3. Start your app using the `Procfile`
4. Provide you with a live URL

### 3.3: Monitor Deployment
1. Go to your Railway project dashboard
2. Click on "Deployments" tab
3. Watch the build logs
4. Your app will be available at the provided URL

## Step 4: Test Your Live App

1. Copy the Railway URL (e.g., `https://your-app-name.railway.app`)
2. Open it in your browser
3. Test a query like "What are the top 5 stores by revenue?"
4. Verify AI summarization works

## Troubleshooting

### Common Issues:

1. **Build fails**: Check the build logs in Railway dashboard
2. **App crashes**: Check the deployment logs
3. **Environment variables**: Make sure all required variables are set
4. **Database connection**: Verify your DATABASE_URL is correct

### Getting Help:

- Check Railway logs in the dashboard
- Verify all environment variables are set correctly
- Make sure your GitHub repo is up to date
- Check that your Neon database is accessible

## Cost Monitoring

Railway's free tier includes:
- $5 credit per month
- Your app should use well under this limit
- Monitor usage in the Railway dashboard

## Next Steps After Deployment

1. **Custom Domain** (optional): Add your own domain in Railway settings
2. **Monitoring**: Set up alerts for your app
3. **Scaling**: Upgrade plan if you need more resources
4. **Backup**: Your data is safely stored in Neon PostgreSQL

## Security Notes

- Your OpenAI API key is secure in Railway's environment variables
- Your database connection is encrypted
- Railway provides HTTPS automatically
- No sensitive data is exposed in your code
