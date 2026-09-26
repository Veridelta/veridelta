.PHONY: install format lint test notebooks docs docs-serve all clean

install:
	uv sync --all-extras
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg

format:
	uv run ruff format src/ tests/
	uv run ruff check src/ tests/ --fix --select I

lint:
	uv run ruff check src/ tests/
	uv run mypy src/ tests/
	uv run pyright src/

test:
	uv run pytest tests/ --cov=src/veridelta --cov-report=term-missing
	uv run coverage report --include='src/veridelta/engine.py,src/veridelta/models.py,src/veridelta/sentinels.py,src/veridelta/connectors/sql.py,src/veridelta/connectors/warehouse.py,src/veridelta/connectors/lakehouse.py' --fail-under=100

notebooks:
	uv run pytest tests/notebooks --no-cov

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