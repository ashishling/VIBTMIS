#!/usr/bin/env python3
"""
Local LLM Summarizer for BTC Store Analytics
Supports multiple local LLM backends for result summarization.
"""

import json
import subprocess
import requests
from typing import Dict, Any, Optional
import os

class LocalLLMSummarizer:
    """Interface for various local LLM backends."""
    
    def __init__(self, backend: str = "ollama", model: str = "llama3.2"):
        self.backend = backend.lower()
        self.model = model
        self.base_url = "http://localhost:11434"  # Default Ollama URL
    
    def summarize_query_results(self, query: str, sql: str, results: str) -> str:
        """Generate a summary of query results using local LLM."""
        
        prompt = f"""You are a business analyst helping to interpret retail store data. 

QUERY: {query}
SQL: {sql}

RESULTS:
{results}

Please provide a concise business summary (2-3 paragraphs) that includes:
1. Key findings and insights
2. Notable patterns or trends
3. Actionable recommendations

Focus on business value and avoid technical jargon. Be specific with numbers and store names where relevant.
"""
        
        if self.backend == "ollama":
            return self._query_ollama(prompt)
        elif self.backend == "llamafile":
            return self._query_llamafile(prompt)
        elif self.backend == "openai_compatible":
            return self._query_openai_compatible(prompt)
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")
    
    def _query_ollama(self, prompt: str) -> str:
        """Query Ollama local LLM using the chat API."""
        try:
            url = f"{self.base_url}/api/chat"
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "stream": False
            }
            
            response = requests.post(url, json=payload, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("message", {}).get("content", "No response generated")
                return content
            else:
                return f"Ollama API error: {response.status_code} - {response.text}"
                
        except requests.exceptions.RequestException as e:
            return f"Error connecting to Ollama: {e}"
        except Exception as e:
            return f"Error generating summary: {e}"
    
    def _query_llamafile(self, prompt: str) -> str:
        """Query llamafile local LLM."""
        try:
            # llamafile typically runs on port 8080
            url = "http://localhost:8080/v1/chat/completions"
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 1000
            }
            
            response = requests.post(url, json=payload, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
            
        except requests.exceptions.RequestException as e:
            return f"Error connecting to llamafile: {e}"
        except Exception as e:
            return f"Error generating summary: {e}"
    
    def _query_openai_compatible(self, prompt: str) -> str:
        """Query any OpenAI-compatible local LLM."""
        try:
            # Configure your local LLM endpoint
            url = os.getenv("LOCAL_LLM_URL", "http://localhost:8000/v1/chat/completions")
            
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 1000
            }
            
            response = requests.post(url, json=payload, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
            
        except requests.exceptions.RequestException as e:
            return f"Error connecting to local LLM: {e}"
        except Exception as e:
            return f"Error generating summary: {e}"

def check_ollama_status() -> bool:
    """Check if Ollama is running and has the model."""
    try:
        response = requests.get("http://localhost:11434/api/version", timeout=5)
        if response.status_code == 200:
            return True
    except:
        pass
    
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=5)
        return response.status_code == 200
    except:
        return False

def get_available_models() -> list:
    """Get list of available Ollama models."""
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get("models", [])
            return [model["name"] for model in models]
        return []
    except:
        return []

if __name__ == "__main__":
    # Example usage
    summarizer = LocalLLMSummarizer(backend="ollama", model="mistral:latest")
    
    # Check if Ollama is running
    if check_ollama_status():
        print("✅ Ollama is running")
        models = get_available_models()
        print(f"📋 Available models: {models}")
    else:
        print("❌ Ollama is not running. Please start it first.")
        print("💡 Install Ollama: https://ollama.ai")
        print("💡 Run: ollama pull mistral")
