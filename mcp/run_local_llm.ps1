# Launch the local-LLM MCP chat: qwen3-coder:30b + rlc read-only MCP server.
#
# --provider-url is REQUIRED on this machine: OLLAMA_HOST is set to 0.0.0.0:11434
# (the SERVER bind address, so Ollama serves over the network). mcphost would
# otherwise reuse that scheme-less bind form as its client base URL and fail with
# "first path segment in URL cannot contain colon". Do not "fix" OLLAMA_HOST —
# adding http://127.0.0.1 there would re-bind the Ollama server to localhost only.
& "$env:USERPROFILE\go\bin\mcphost.exe" `
    -m ollama:qwen3-coder:30b `
    --provider-url http://127.0.0.1:11434 `
    --config C:\dev\RLC-Agent\mcp\mcphost-config.json
