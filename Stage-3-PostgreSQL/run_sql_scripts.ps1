# Run SQL scripts in PostgreSQL container
# Usage: .\run_sql_scripts.ps1

Write-Host "`n=== Running SQL Scripts ===" -ForegroundColor Cyan

# Script 1: Create Schemas
Write-Host "`n[1/3] Creating schemas..." -ForegroundColor Yellow
Get-Content sql\schemas\01_create_schemas.sql | docker exec -i postgres-db psql -U postgres -d ecommerce_analytics
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Schemas created successfully" -ForegroundColor Green
} else {
    Write-Host "❌ Schema creation failed" -ForegroundColor Red
    exit 1
}

# Script 2: Create Tables
Write-Host "`n[2/3] Creating tables..." -ForegroundColor Yellow
Get-Content sql\schemas\02_create_tables.sql | docker exec -i postgres-db psql -U postgres -d ecommerce_analytics
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Tables created successfully" -ForegroundColor Green
} else {
    Write-Host "❌ Table creation failed" -ForegroundColor Red
    exit 1
}

# Script 3: Create Indexes (optional)
if (Test-Path sql\schemas\03_create_indexes.sql) {
    Write-Host "`n[3/3] Creating indexes..." -ForegroundColor Yellow
    Get-Content sql\schemas\03_create_indexes.sql | docker exec -i postgres-db psql -U postgres -d ecommerce_analytics
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Indexes created successfully" -ForegroundColor Green
    } else {
        Write-Host "❌ Index creation failed" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n=== SQL Scripts Completed ===" -ForegroundColor Cyan

# Verify tables
Write-Host "`n📋 Verifying tables..." -ForegroundColor Yellow
docker exec postgres-db psql -U postgres -d ecommerce_analytics -c "\dt raw.*; \dt staging.*; \dt analytics.*;"

Write-Host "`n✅ Database setup complete!" -ForegroundColor Green