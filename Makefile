.PHONY: install format lint test notebooks postgres docs docs-serve schema all clean

install:
	uv sync --all-extras
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg

format:
	uv run ruff format src/ tests/
	uv run ruff check src/ tests/ --select I

lint:
	uv run ruff check src/ tests/
	uv run mypy src/ tests/
	uv run pyright src/

test:
	uv run pytest tests/
	uv run coverage report --include='src/veridelta/engine.py,src/veridelta/models.py,src/veridelta/sentinels.py,src/veridelta/telemetry.py,src/veridelta/connectors/sql.py,src/veridelta/connectors/warehouse.py,src/veridelta/connectors/lakehouse.py,src/veridelta/connectors/database.py,src/veridelta/connectors/duckdb.py' --fail-under=100

notebooks:
	uv run pytest tests/notebooks --no-cov

postgres:
	@test -n "$$VERIDELTA_POSTGRES_URI" || { echo "Set VERIDELTA_POSTGRES_URI to a Postgres server the tests may create tables on."; exit 1; }
	VERIDELTA_PARITY_BACKEND=postgres uv run pytest tests/integration/test_pushdown_parity.py tests/integration/test_parity_fuzz.py tests/integration/test_postgres_pushdown.py -m "not duckdb_only" --no-cov

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