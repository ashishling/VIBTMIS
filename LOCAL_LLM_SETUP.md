# Local LLM Setup Guide

This guide shows you how to set up a local LLM for AI-powered result summarization in your BTC Store Analytics app.

## Option 1: Ollama (Recommended)

### Installation

1. **Install Ollama:**
   ```bash
   # macOS
   brew install ollama
   
   # Or download from: https://ollama.ai
   ```

2. **Start Ollama:**
   ```bash
   ollama serve
   ```

3. **Download a model:**
   ```bash
   # Recommended models (pick one):
   ollama pull llama3.2          # Fast, good quality (2GB)
   ollama pull llama3.2:3b       # Very fast (2GB)
   ollama pull mistral           # Excellent quality (4GB)
   ollama pull codellama         # Good for technical analysis (4GB)
   ```

4. **Test the setup:**
   ```bash
   python3 local_llm_summarizer.py
   ```

### Usage

Once Ollama is running, your web app will automatically detect it and show:
- ✅ **Local LLM (Ollama) Connected** status
- 🤖 **Summarize Results** button after each query

## Option 2: llamafile

### Installation

1. **Download llamafile:**
   ```bash
   # Download from: https://github.com/Mozilla-Ocho/llamafile
   wget https://github.com/Mozilla-Ocho/llamafile/releases/download/0.8.2/llamafile-0.8.2
   chmod +x llamafile-0.8.2
   ```

2. **Run with a model:**
   ```bash
   ./llamafile-0.8.2 -m llama-2-7b-chat.Q4_K_M.llamafile --server --port 8080
   ```

3. **Update the summarizer:**
   ```python
   # In local_llm_summarizer.py, change:
   summarizer = LocalLLMSummarizer(backend="llamafile", model="llama-2-7b-chat")
   ```

## Option 3: Any OpenAI-Compatible API

If you have another local LLM server running:

1. **Set the environment variable:**
   ```bash
   export LOCAL_LLM_URL="http://localhost:8000/v1/chat/completions"
   ```

2. **Update the summarizer:**
   ```python
   summarizer = LocalLLMSummarizer(backend="openai_compatible", model="your-model-name")
   ```

## Testing Your Setup

1. **Check if Ollama is running:**
   ```bash
   curl http://localhost:11434/api/tags
   ```

2. **Test the summarizer directly:**
   ```bash
   python3 local_llm_summarizer.py
   ```

3. **Test in the web app:**
   - Go to http://localhost:8080
   - Ask a question: "What are the top 5 stores by revenue?"
   - Click the "🤖 Summarize Results" button

## Model Recommendations

### For Business Analysis:
- **llama3.2** - Good balance of speed and quality
- **mistral** - Excellent reasoning capabilities
- **qwen2** - Great for financial analysis

### For Technical Analysis:
- **codellama** - Understands SQL and data patterns
- **deepseek-coder** - Good for technical summaries

### For Speed:
- **llama3.2:3b** - Very fast, decent quality
- **phi3** - Microsoft's efficient model

## Troubleshooting

### Ollama not starting:
```bash
# Check if port 11434 is in use
lsof -i :11434

# Kill existing processes
pkill ollama

# Restart
ollama serve
```

### Model not found:
```bash
# List available models
ollama list

# Pull a specific model
ollama pull llama3.2
```

### Connection refused:
- Ensure Ollama is running: `ollama serve`
- Check firewall settings
- Verify port 11434 is accessible

## Performance Tips

1. **Use smaller models** for faster responses (3B parameters)
2. **Close unused models** to free memory: `ollama stop model_name`
3. **Use SSD storage** for better model loading speed
4. **Allocate sufficient RAM** (8GB+ recommended)

## Privacy & Security

✅ **Your data stays local** - no data sent to external servers
✅ **No API keys required** - completely offline
✅ **Full control** - you own the entire pipeline

This setup ensures your sensitive business data never leaves your machine while still providing powerful AI insights!
