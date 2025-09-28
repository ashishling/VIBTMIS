#!/usr/bin/env python3
"""
BTC Store Data Analytics Web UI
Simple Flask web interface for natural language to SQL queries.
"""

from flask import Flask, render_template, request, jsonify
import os
from dotenv import load_dotenv
from nl_to_sql_postgres import get_openai_client, generate_sql_query, execute_sql_query
from local_llm_summarizer import LocalLLMSummarizer, check_ollama_status, get_available_models
import traceback

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize OpenAI client
try:
    openai_client = get_openai_client()
    api_connected = True
except Exception as e:
    api_connected = False
    error_message = str(e)

# Initialize Local LLM summarizer
try:
    local_llm_summarizer = LocalLLMSummarizer(backend="ollama", model="mistral:latest")
    local_llm_available = check_ollama_status()
except Exception as e:
    local_llm_available = False

@app.route('/')
def index():
    """Main page with the query interface."""
    return render_template('index.html', api_connected=api_connected, local_llm_available=local_llm_available)

@app.route('/api/query', methods=['POST'])
def process_query():
    """Process natural language query and return results."""
    try:
        if not api_connected:
            return jsonify({
                'success': False,
                'error': 'OpenAI API not connected. Please check your API key.',
                'details': error_message
            })
        
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Please enter a question.'
            })
        
        # Generate SQL query
        sql_query = generate_sql_query(query, openai_client)
        
        # Execute the query
        results = execute_sql_query(sql_query)
        
        return jsonify({
            'success': True,
            'sql_query': sql_query,
            'results': results,
            'query': query
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'details': traceback.format_exc()
        })

@app.route('/api/status')
def api_status():
    """Check API connection status."""
    return jsonify({
        'api_connected': api_connected,
        'local_llm_available': local_llm_available,
        'error': error_message if not api_connected else None
    })

@app.route('/api/summarize', methods=['POST'])
def summarize_results():
    """Summarize query results using local LLM."""
    try:
        if not local_llm_available:
            return jsonify({
                'success': False,
                'error': 'Local LLM not available. Please ensure Ollama is running.'
            })
        
        data = request.get_json()
        query = data.get('query', '')
        sql = data.get('sql', '')
        results = data.get('results', '')
        
        if not all([query, sql, results]):
            return jsonify({
                'success': False,
                'error': 'Missing required data: query, sql, or results'
            })
        
        # Generate summary using local LLM
        summary = local_llm_summarizer.summarize_query_results(query, sql, results)
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/models')
def get_models():
    """Get available local LLM models."""
    try:
        models = get_available_models()
        return jsonify({
            'success': True,
            'models': models
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/examples')
def examples():
    """Show example queries."""
    example_queries = [
        "What are the top 10 stores by revenue in 2024?",
        "Show me monthly transaction trends",
        "Which region has the highest EBITDA?",
        "What's the average store area by category?",
        "Show me revenue per square foot for each store",
        "Which stores have the highest profit margins?",
        "Compare CWK vs SIS/Others performance",
        "Show me quarterly revenue trends by vintage cohort",
        "What's the growth rate of transactions year over year?",
        "Which vintage cohort performs best?"
    ]
    
    return render_template('examples.html', examples=example_queries)

if __name__ == '__main__':
    print("🚀 Starting BT MIS Analytics Web UI...")
    
    # Get port from environment variable (Railway sets this)
    port = int(os.environ.get('PORT', 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    if debug_mode:
        print("📊 Open your browser and go to: http://localhost:8080")
    else:
        print("🌐 App is running in production mode")
    
    print("🔒 Privacy-first: Your data stays local, OpenAI only generates SQL")
    print("🔑 Make sure your OpenAI API key is set in .env file")
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port)
