.PHONY: install format lint test all clean

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

test:
	uv run pytest tests/ --cov=src/veridelta --cov-report=term-missing

all: format lint test

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	rm -rf site/ dist/ build/
	rm -f .coverage coverage.xml
	find . -type d -name "__pycache__" -exec rm -rf {} +