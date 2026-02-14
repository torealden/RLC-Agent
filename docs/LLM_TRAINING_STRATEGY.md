# RLC LLM Training Strategy: Building an AI Agricultural Economist

## Executive Summary

**Good news: You are NOT locked in.** Your current architecture (Ollama + tool-calling + RAG + SQLite) is highly flexible and positions you well for multiple training approaches. The key question is not "can we do this?" but "what's the right sequence of steps?"

## The Three Approaches

Based on current research and best practices, here's the decision framework:

### Approach 1: Enhanced RAG + Tool Calling (Recommended Starting Point)
**Effort: Low | Time: Days | No GPU Required**

This approach gives you 70-80% of what you want with minimal effort:

1. **Knowledge Base Documents**: Create detailed documents explaining:
   - Balance sheet relationships (Beginning Stocks + Production + Imports = Total Supply)
   - How to identify reputable data sources
   - USDA vs local source guidelines
   - Data quality standards

2. **Enhanced Tools**: Add tools for:
   - USDA PSD API access (free, just needs API key)
   - Web search for new data sources
   - Data validation and cleaning
   - Schema generation

3. **Structured Prompts**: System prompts that include:
   - Balance sheet formulas
   - Decision trees for data sourcing
   - Approval workflow steps

**Why start here?** Research shows RAG reduces hallucinations by grounding responses in retrieved documents. For dynamic, factual data like commodity prices, RAG is often *better* than fine-tuning.

### Approach 2: Fine-Tuning with QLoRA (Phase 2)
**Effort: Medium | Time: Weeks | GPU Required (16GB+ VRAM)**

When to add fine-tuning:
- When you want the model to "think like an ag economist" without prompting
- When you have 1,000+ examples of desired behavior
- When response style/format needs to be consistent

**Requirements:**
- GPU with 16GB+ VRAM (RTX 4090 ideal, RTX 3080/4080 workable)
- Training data: Q&A pairs, balance sheet examples, analysis samples
- Tools: Unsloth (easiest), Axolotl (more control), or torchtune

**Training Data You Already Have:**
- 68,286+ data points in your database
- 100+ Excel spreadsheets with your analysis methodology
- Your voice recordings (future) explaining your thought process

### Approach 3: Continued Pre-Training (Phase 3 - Future)
**Effort: High | Time: Months | Significant GPU Required**

This involves training the model on agricultural text corpora:
- USDA reports, academic papers, industry publications
- Your historical analyses and reports
- Market commentary and forecasts

**Only consider this when:**
- Approaches 1 & 2 aren't sufficient
- You have significant compute budget
- You want to create a truly specialized "RLC Model"

---

## Your Specific Questions Answered

### Q1: Can the LLM learn to strip/extract from these files?
**YES.** This is already working! Your `fast_extractor.py` does this. To teach the LLM to do it:
1. Create a tool that wraps the extraction logic
2. Add it to your agent's toolset
3. The LLM can then call it when asked to "extract data from [file]"

### Q2: Can the LLM learn balance sheet relationships?
**YES.** Multiple approaches:

**Immediate (RAG):**
```
Store this in your knowledge base:
"Balance Sheet Identity:
Beginning Stocks + Production + Imports = Total Supply
Total Supply = Domestic Consumption + Exports + Ending Stocks

Therefore:
Ending Stocks = Beginning Stocks + Production + Imports - Domestic Consumption - Exports"
```

**Better (Examples in Context):**
Include worked examples in your system prompt or retrieved documents.

**Best (Fine-tuning):**
Train on 1,000+ examples of balance sheet Q&A.

### Q3: Can it autonomously find new markets and data sources?
**YES, with tools.** The architecture would be:

1. **Discovery Tool**: Web search for "[commodity] [country] production data"
2. **Source Evaluation Tool**: Check if source is USDA, FAO, or known ministry
3. **API Integration Tool**: Configure API connections
4. **Extraction Tool**: Download and clean data
5. **Schema Tool**: Design database schema
6. **Human Approval Gate**: Present plan for your approval

Initially, every step requires approval. Over time, you automate more.

### Q4: Is downloading Llama still viable?
**YES, absolutely.** The ecosystem has matured significantly:

| Model | Size | Memory Needed | Notes |
|-------|------|---------------|-------|
| Llama 3.2 3B | 3B | 4GB | Fast, good for simple tasks |
| Llama 3.1 8B | 8B | 8GB | Best balance for local use |
| Llama 3.3 70B | 70B | 48GB+ | Near-GPT-4 quality, needs quantization |
| Qwen 2.5 | Various | Various | Excellent at structured tasks |

**For your use case:** Llama 3.1 8B or Qwen 2.5 7B are ideal starting points.

### Q5: Are we locked into a path we can't change?
**NO.** Your architecture is remarkably flexible:

- **Database**: SQLite can migrate to PostgreSQL, MySQL, or cloud DBs easily
- **LLM**: Ollama supports any GGUF model; can swap models anytime
- **Tools**: Tool-calling pattern works with any LLM that supports it
- **RAG**: ChromaDB can be replaced with any vector store
- **Code**: Python scripts are modular and portable

---

## Recommended Roadmap

### Phase 1: Complete Data Foundation (This Week)
1. ✅ Extract Soybean, Sunflower, Rapeseed data
2. ⬜ Extract remaining files (Lauric, Peanut, Vegetable Oil, US Oilseed)
3. ⬜ Add USDA PSD API integration
4. ⬜ Create PowerBI dashboards

### Phase 2: Knowledge Base & Tools (Next 2-4 Weeks)
1. Create balance sheet knowledge documents
2. Add data source discovery tools
3. Add USDA API tools
4. Record voice explanations → transcribe → add to RAG
5. Test with your LLM agent

### Phase 3: Evaluation & Fine-Tuning Prep (Weeks 4-8)
1. Test the enhanced system
2. Identify gaps where prompting isn't enough
3. Create training dataset from:
   - Your voice transcriptions
   - Q&A pairs about balance sheets
   - Examples of good/bad analysis
4. Set up fine-tuning infrastructure

### Phase 4: Fine-Tuning (When Ready)
1. Start with Llama 3.1 8B + QLoRA
2. Train on your dataset
3. Merge adapter weights
4. Deploy to Ollama
5. Iterate based on results

---

## Hardware Recommendations for RLC-SERVER

For fine-tuning capability, you'd want:

**Minimum:**
- NVIDIA RTX 3080/3090 (10-24GB VRAM)
- 32GB RAM
- 500GB SSD

**Ideal:**
- NVIDIA RTX 4090 (24GB VRAM)
- 64GB RAM
- 1TB NVMe SSD

**Note:** If RLC-SERVER doesn't have a GPU, you can:
1. Use Google Colab (free GPU access)
2. Rent GPU time (Lambda Labs, Vast.ai, RunPod)
3. Use cloud fine-tuning services (Together.ai, Anyscale)

---

## The Voice Recording Approach

Your idea to "download your brain" via voice recordings is excellent. Here's the workflow:

1. **Record**: Talk through your analysis process while updating spreadsheets
2. **Transcribe**: Use Whisper (runs locally via Ollama or standalone)
3. **Structure**: Convert transcriptions to:
   - Q&A pairs (for fine-tuning)
   - Knowledge documents (for RAG)
   - Process descriptions (for system prompts)
4. **Integrate**: Add to your LLM's knowledge base

This captures tacit knowledge that's impossible to get any other way.

---

## Immediate Next Steps

1. **Extract Remaining Balance Sheets** (30 minutes)
   - Run fast_extractor on remaining files

2. **Add USDA API Tool** (1-2 hours)
   - Get API key from api.data.gov
   - Add tool to query PSD database

3. **Create Balance Sheet Knowledge Doc** (1 hour)
   - Document the relationships and formulas
   - Add to RAG system

4. **Test Current Capability** (ongoing)
   - Ask your LLM to explain balance sheet relationships
   - Ask it to find data for a new commodity
   - Identify gaps

5. **Start Recording** (whenever ready)
   - Record yourself explaining your analysis
   - We'll set up transcription pipeline

---

## Research Sources

### Fine-Tuning Best Practices
- [How to fine-tune open LLMs in 2025](https://www.philschmid.de/fine-tune-llms-in-2025)
- [Fine-tuning Llama 3 with QLoRA](https://rabiloo.com/blog/a-step-by-step-guide-to-fine-tuning-llama-3-using-lora-and-qlora)
- [Unsloth + Ollama Tutorial](https://docs.unsloth.ai/get-started/fine-tuning-llms-guide/tutorial-how-to-finetune-llama-3-and-use-in-ollama)
- [DataCamp: Fine-Tuning Llama 3 Locally](https://www.datacamp.com/tutorial/llama3-fine-tuning-locally)

### RAG vs Fine-Tuning
- [IBM: RAG vs Fine-tuning](https://www.ibm.com/think/topics/rag-vs-fine-tuning)
- [Oracle: How to Choose](https://www.oracle.com/artificial-intelligence/generative-ai/retrieval-augmented-generation-rag/rag-fine-tuning/)
- [ArXiv: RAG vs Fine-tuning Case Study on Agriculture](https://arxiv.org/abs/2401.08406)

### Agricultural LLMs
- [LLMs in Agriculture Research](https://link.springer.com/article/10.1007/s13748-024-00359-4)
- [Knowledge-Guided Agricultural LLM](https://www.sciencedirect.com/science/article/abs/pii/S0950705125002448)

### USDA Data APIs
- [USDA FAS OpenData Portal](https://apps.fas.usda.gov/opendatawebV2/#/home)
- [PSD Database API Documentation](https://data.nal.usda.gov/dataset/usda-foreign-agricultural-service-production-supply-and-distribution-database)
- [ERS Data APIs](https://www.ers.usda.gov/developer/data-apis)
