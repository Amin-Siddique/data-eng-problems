"""SQL Editor web UI for Lakehouse Local."""

import os
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pyspark.sql import SparkSession

app = FastAPI(title="Lakehouse Local SQL Editor")

SPARK_HOST = os.environ.get("SPARK_HOST", "localhost")
SPARK_PORT = os.environ.get("SPARK_PORT", "15002")


def get_spark() -> SparkSession:
    """Get or create Spark session connected to the local lakehouse."""
    return (
        SparkSession.builder
        .remote(f"sc://{SPARK_HOST}:{SPARK_PORT}")
        .appName("LakehouseLocal-UI")
        .getOrCreate()
    )


class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int
    execution_time_ms: int
    error: Optional[str] = None


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the SQL editor UI."""
    return HTML_TEMPLATE


@app.post("/api/query", response_model=QueryResult)
async def execute_query(request: QueryRequest):
    """Execute a SQL query and return results."""
    start_time = time.time()

    try:
        spark = get_spark()
        df = spark.sql(request.sql)

        # Limit results for UI
        rows = df.limit(request.limit).collect()
        columns = df.columns

        execution_time_ms = int((time.time() - start_time) * 1000)

        return QueryResult(
            columns=columns,
            rows=[[str(cell) for cell in row] for row in rows],
            row_count=len(rows),
            execution_time_ms=execution_time_ms,
        )
    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        return QueryResult(
            columns=[],
            rows=[],
            row_count=0,
            execution_time_ms=execution_time_ms,
            error=str(e),
        )


@app.get("/api/tables")
async def list_tables():
    """List all available tables."""
    try:
        spark = get_spark()

        # Get tables from all catalogs
        catalogs = spark.sql("SHOW CATALOGS").collect()
        tables = []

        for catalog in catalogs:
            catalog_name = catalog[0]
            try:
                schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
                for schema in schemas:
                    schema_name = schema[0]
                    try:
                        tbls = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
                        for tbl in tbls:
                            tables.append({
                                "catalog": catalog_name,
                                "schema": schema_name,
                                "table": tbl.tableName,
                                "full_name": f"{catalog_name}.{schema_name}.{tbl.tableName}"
                            })
                    except Exception:
                        pass
            except Exception:
                pass

        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
async def health():
    """Health check endpoint."""
    try:
        spark = get_spark()
        spark.sql("SELECT 1").collect()
        return {"status": "healthy", "spark": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lakehouse Local - SQL Editor</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #1a1a2e;
            color: #eee;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }

        header {
            background: #16213e;
            padding: 1rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #0f3460;
        }

        header h1 {
            font-size: 1.5rem;
            color: #e94560;
        }

        .status {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #4ade80;
        }

        .status-dot.error {
            background: #f87171;
        }

        main {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 1rem;
            gap: 1rem;
            overflow: hidden;
        }

        .editor-section {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        textarea {
            width: 100%;
            height: 150px;
            background: #0f0f23;
            border: 1px solid #0f3460;
            border-radius: 8px;
            color: #eee;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 14px;
            padding: 1rem;
            resize: vertical;
        }

        textarea:focus {
            outline: none;
            border-color: #e94560;
        }

        .controls {
            display: flex;
            gap: 1rem;
            align-items: center;
        }

        button {
            background: #e94560;
            color: white;
            border: none;
            padding: 0.75rem 2rem;
            border-radius: 6px;
            font-size: 1rem;
            cursor: pointer;
            transition: background 0.2s;
        }

        button:hover {
            background: #ff6b6b;
        }

        button:disabled {
            background: #666;
            cursor: not-allowed;
        }

        .meta {
            color: #888;
            font-size: 0.875rem;
        }

        .results-section {
            flex: 1;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        .results-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.5rem;
        }

        .results-container {
            flex: 1;
            overflow: auto;
            background: #0f0f23;
            border: 1px solid #0f3460;
            border-radius: 8px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
        }

        th, td {
            padding: 0.75rem 1rem;
            text-align: left;
            border-bottom: 1px solid #1a1a2e;
        }

        th {
            background: #16213e;
            position: sticky;
            top: 0;
            font-weight: 600;
        }

        tr:hover td {
            background: #1a1a2e;
        }

        .error-message {
            background: #7f1d1d;
            color: #fca5a5;
            padding: 1rem;
            border-radius: 8px;
            font-family: monospace;
            white-space: pre-wrap;
        }

        .sidebar {
            position: fixed;
            right: 0;
            top: 60px;
            bottom: 0;
            width: 300px;
            background: #16213e;
            border-left: 1px solid #0f3460;
            padding: 1rem;
            transform: translateX(100%);
            transition: transform 0.3s;
            overflow-y: auto;
        }

        .sidebar.open {
            transform: translateX(0);
        }

        .sidebar h3 {
            margin-bottom: 1rem;
            color: #e94560;
        }

        .table-item {
            padding: 0.5rem;
            cursor: pointer;
            border-radius: 4px;
            font-size: 0.875rem;
            font-family: monospace;
        }

        .table-item:hover {
            background: #0f3460;
        }

        .toggle-sidebar {
            position: fixed;
            right: 1rem;
            bottom: 1rem;
            background: #0f3460;
            border: none;
            padding: 0.75rem;
            border-radius: 50%;
            cursor: pointer;
            color: #eee;
        }
    </style>
</head>
<body>
    <header>
        <h1>Lakehouse Local</h1>
        <div class="status">
            <span class="status-dot" id="statusDot"></span>
            <span id="statusText">Connecting...</span>
        </div>
    </header>

    <main>
        <div class="editor-section">
            <textarea id="sqlEditor" placeholder="-- Write your SQL here
SELECT * FROM samples.tpch.lineitem LIMIT 10;"></textarea>
            <div class="controls">
                <button id="runBtn" onclick="runQuery()">Run Query (Ctrl+Enter)</button>
                <span class="meta" id="execTime"></span>
            </div>
        </div>

        <div class="results-section">
            <div class="results-header">
                <h3>Results</h3>
                <span class="meta" id="rowCount"></span>
            </div>
            <div class="results-container" id="results">
                <p style="padding: 1rem; color: #888;">Run a query to see results</p>
            </div>
        </div>
    </main>

    <div class="sidebar" id="sidebar">
        <h3>Tables</h3>
        <div id="tableList">Loading...</div>
    </div>

    <button class="toggle-sidebar" onclick="toggleSidebar()">
        <svg width="24" height="24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M3 12h18M3 6h18M3 18h18"/>
        </svg>
    </button>

    <script>
        const editor = document.getElementById('sqlEditor');
        const runBtn = document.getElementById('runBtn');
        const results = document.getElementById('results');
        const execTime = document.getElementById('execTime');
        const rowCount = document.getElementById('rowCount');
        const statusDot = document.getElementById('statusDot');
        const statusText = document.getElementById('statusText');

        // Check health on load
        async function checkHealth() {
            try {
                const res = await fetch('/api/health');
                const data = await res.json();
                if (data.status === 'healthy') {
                    statusDot.classList.remove('error');
                    statusText.textContent = 'Connected';
                } else {
                    statusDot.classList.add('error');
                    statusText.textContent = 'Disconnected';
                }
            } catch (e) {
                statusDot.classList.add('error');
                statusText.textContent = 'Disconnected';
            }
        }

        // Load tables
        async function loadTables() {
            try {
                const res = await fetch('/api/tables');
                const data = await res.json();
                const tableList = document.getElementById('tableList');
                tableList.innerHTML = data.tables.map(t =>
                    `<div class="table-item" onclick="insertTable('${t.full_name}')">${t.full_name}</div>`
                ).join('');
            } catch (e) {
                document.getElementById('tableList').innerHTML = 'Failed to load tables';
            }
        }

        function insertTable(name) {
            editor.value = `SELECT * FROM ${name} LIMIT 100;`;
            editor.focus();
        }

        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('open');
        }

        async function runQuery() {
            const sql = editor.value.trim();
            if (!sql) return;

            runBtn.disabled = true;
            runBtn.textContent = 'Running...';
            results.innerHTML = '<p style="padding: 1rem; color: #888;">Executing query...</p>';

            try {
                const res = await fetch('/api/query', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sql, limit: 1000})
                });
                const data = await res.json();

                execTime.textContent = `${data.execution_time_ms}ms`;

                if (data.error) {
                    results.innerHTML = `<div class="error-message">${data.error}</div>`;
                    rowCount.textContent = '';
                } else if (data.rows.length === 0) {
                    results.innerHTML = '<p style="padding: 1rem; color: #888;">Query returned no results</p>';
                    rowCount.textContent = '0 rows';
                } else {
                    rowCount.textContent = `${data.row_count} rows`;
                    results.innerHTML = `
                        <table>
                            <thead>
                                <tr>${data.columns.map(c => `<th>${c}</th>`).join('')}</tr>
                            </thead>
                            <tbody>
                                ${data.rows.map(row =>
                                    `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`
                                ).join('')}
                            </tbody>
                        </table>
                    `;
                }
            } catch (e) {
                results.innerHTML = `<div class="error-message">Network error: ${e.message}</div>`;
            }

            runBtn.disabled = false;
            runBtn.textContent = 'Run Query (Ctrl+Enter)';
        }

        // Keyboard shortcut
        editor.addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                runQuery();
            }
        });

        // Initialize
        checkHealth();
        loadTables();
        setInterval(checkHealth, 30000);
    </script>
</body>
</html>
"""
