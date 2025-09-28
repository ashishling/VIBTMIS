#!/usr/bin/env python3
"""
Interactive Natural Language to SQL Query Interface
Allows users to ask questions in natural language and get SQL results.

Usage:
    python interactive_query.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from nl_to_sql import get_openai_client, generate_sql_query, execute_sql_query

# Load environment variables from .env file
load_dotenv()

def main():
    print("🤖 BTC Store Data Query Interface")
    print("Ask questions about your store data in natural language!")
    print("Type 'quit' or 'exit' to stop.")
    print("=" * 60)
    
    try:
        # Initialize OpenAI client
        client = get_openai_client()
        print("✅ Connected to OpenAI API")
        
    except Exception as e:
        print(f"❌ Error connecting to OpenAI: {e}")
        print("Please set your OpenAI API key:")
        print("export OPENAI_API_KEY='your-api-key-here'")
        return
    
    while True:
        try:
            # Get user input
            user_query = input("\n💬 Your question: ").strip()
            
            if user_query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not user_query:
                continue
            
            print(f"\n🤖 Converting: '{user_query}'")
            print("⏳ Generating SQL...")
            
            # Generate SQL query
            sql_query = generate_sql_query(user_query, client)
            
            print("✅ Generated SQL:")
            print("-" * 40)
            print(sql_query)
            print("-" * 40)
            
            # Ask if user wants to execute
            execute = input("\n🚀 Execute this query? (y/n): ").strip().lower()
            
            if execute in ['y', 'yes', '']:
                print("\n🔄 Executing query...")
                result = execute_sql_query(sql_query)
                
                print("📊 Results:")
                print("=" * 50)
                print(result)
            else:
                print("📝 SQL query generated but not executed")
        
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("Please try again with a different question.")

if __name__ == "__main__":
    main()
