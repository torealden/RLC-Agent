Write-Host "=== Country Reconciliation (12 tasks) ===" -ForegroundColor Cyan
python scripts/ollama/task_runner.py scripts/ollama/jobs/country_reconciliation.yaml

Write-Host "`n=== Data Validation Queries (8 tasks) ===" -ForegroundColor Cyan
python scripts/ollama/task_runner.py scripts/ollama/jobs/data_validation_queries.yaml

Write-Host "`n=== All done ===" -ForegroundColor Green
