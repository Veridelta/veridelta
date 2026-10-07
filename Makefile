.PHONY: install format lint test notebooks postgres live databases accessibility docs docs-serve schema all clean demo

# The modules held to full branch coverage. CI's core-module gate names the same
# list, which tests/unit/test_ci_integrations.py checks.
CORE_MODULES := src/veridelta/engine.py,src/veridelta/models.py,src/veridelta/sentinels.py,src/veridelta/telemetry.py,src/veridelta/connectors/sql.py,src/veridelta/connectors/warehouse.py,src/veridelta/connectors/lakehouse.py,src/veridelta/connectors/database.py,src/veridelta/connectors/duckdb.py,src/veridelta/mcp_server.py

install:
	uv sync --all-extras
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg

format:
	uv run ruff format src/ tests/ hooks/
	uv run ruff check src/ tests/ hooks/ --select I

lint:
	uv run ruff check src/ tests/ hooks/
	uv run mypy src/ tests/ hooks/
	uv run pyright src/

test:
	uv run pytest tests/
	uv run coverage report --include='$(CORE_MODULES)' --fail-under=100

notebooks:
	uv run pytest tests/notebooks --no-cov

postgres:
	@test -n "$$VERIDELTA_POSTGRES_URI" || { echo "Set VERIDELTA_POSTGRES_URI to a Postgres server the tests may create tables on."; exit 1; }
	VERIDELTA_PARITY_BACKEND=postgres uv run pytest tests/integration/test_pushdown_parity.py tests/integration/test_parity_fuzz.py tests/integration/test_postgres_pushdown.py --no-cov

live:
	@case "$$VERIDELTA_PARITY_BACKEND" in bigquery|databricks|motherduck|snowflake) ;; *) echo "Set VERIDELTA_PARITY_BACKEND to bigquery, databricks, motherduck, or snowflake; see 'Live warehouse tests' in CONTRIBUTING.md."; exit 1;; esac
	uv run pytest tests/integration/test_pushdown_parity.py --no-cov -rs

databases:
	@test -n "$$VERIDELTA_MYSQL_URI$$VERIDELTA_MSSQL_URI" || { echo "Set VERIDELTA_MYSQL_URI or VERIDELTA_MSSQL_URI to a server the tests may create tables on."; exit 1; }
	uv run --group databases pytest tests/integration/test_database_servers.py --no-cov

accessibility:
	VERIDELTA_ACCESSIBILITY=1 uv run --group accessibility pytest tests/accessibility --no-cov

demo:
	cd demo && vhs veridelta.tape

schema:
	uv run veridelta schema > docs/schema/veridelta.schema.json

docs:
	uv run mkdocs build --strict

docs-serve:
	uv run mkdocs serve

all: format lint test docs

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	rm -rf site/ dist/ build/
	rm -f .coverage coverage.xml
	find . -type d -name "__pycache__" -exec rm -rf {} +