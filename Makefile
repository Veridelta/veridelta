.PHONY: install format lint hooks test notebooks postgres live databases accessibility docs docs-serve schema all clean demo demo-video vhs-check screenshots

# The modules held to full branch coverage. CI's core-module gate names the same
# list, which tests/unit/test_ci_integrations.py checks.
CORE_MODULES := src/veridelta/engine.py,src/veridelta/_resolution.py,src/veridelta/_suggest.py,src/veridelta/models.py,src/veridelta/sentinels.py,src/veridelta/telemetry.py,src/veridelta/connectors/sql.py,src/veridelta/connectors/warehouse.py,src/veridelta/connectors/lakehouse.py,src/veridelta/connectors/database.py,src/veridelta/connectors/duckdb.py,src/veridelta/mcp_server.py

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

# The Git hooks on every file, as CI's lint job runs them. `lint` reads the Python
# folders only, so a notebook or a demo script needs this too.
hooks:
	uv run pre-commit run --all-files

test:
	uv run pytest tests/
	uv run coverage report --include='$(CORE_MODULES)' --fail-under=100

notebooks:
	uv run pytest tests/notebooks --no-cov

postgres:
	@test -n "$$VERIDELTA_POSTGRES_URI" || { echo "Set VERIDELTA_POSTGRES_URI to a Postgres server the tests may create tables on."; exit 1; }
	VERIDELTA_PARITY_BACKEND=postgres uv run pytest tests/integration/test_pushdown_parity.py tests/integration/test_seeded_drift.py tests/integration/test_parity_fuzz.py tests/integration/test_postgres_pushdown.py --no-cov

live:
	@case "$$VERIDELTA_PARITY_BACKEND" in bigquery|databricks|motherduck|snowflake) ;; *) echo "Set VERIDELTA_PARITY_BACKEND to bigquery, databricks, motherduck, or snowflake; see 'Live warehouse tests' in CONTRIBUTING.md."; exit 1;; esac
	uv run pytest tests/integration/test_pushdown_parity.py tests/integration/test_seeded_drift.py --no-cov -rs

databases:
	@test -n "$$VERIDELTA_MYSQL_URI$$VERIDELTA_MSSQL_URI" || { echo "Set VERIDELTA_MYSQL_URI or VERIDELTA_MSSQL_URI to a server the tests may create tables on."; exit 1; }
	uv run --group databases pytest tests/integration/test_database_servers.py --no-cov

accessibility:
	VERIDELTA_ACCESSIBILITY=1 uv run --group accessibility pytest tests/accessibility --no-cov

# The vhs release the recordings are made with. Another release can draw the same tape
# differently, so `make demo` refuses it.
VHS_VERSION := v0.12.1

vhs-check:
	@vhs --version 2>/dev/null | grep -qx "vhs version $(VHS_VERSION)" || { \
		echo "Recording needs vhs $(VHS_VERSION), with ttyd and ffmpeg on PATH:"; \
		echo "  go install github.com/charmbracelet/vhs@$(VHS_VERSION)"; \
		echo "See Recording the demos in CONTRIBUTING.md."; \
		exit 1; \
	}

demo: vhs-check
	cd demo && for tape in *.tape; do \
		[ "$$tape" = settings.tape ] || uv run vhs "$$tape" || exit 1; \
	done

# The HTML report in light and dark, and the link preview card, under docs/assets/.
screenshots:
	uv run --group accessibility python demo/screenshots.py

# MP4 copies of the recordings for promotional videos, in demo/video/, which git ignores.
# Each renders from a copy of its tape whose only output is the MP4, so no GIF changes.
# The tapes in demo/promo/ are made for video alone, in a larger font, and write their
# MP4 themselves. Last comes the HTML report of the accounts run, as an image for video.
demo-video: vhs-check
	cd demo && mkdir -p video && for tape in *.tape; do \
		[ "$$tape" = settings.tape ] && continue; \
		sed "s|^Output .*|Output video/$${tape%.tape}.mp4|" "$$tape" > "video/$$tape" && \
		uv run vhs "video/$$tape" || exit 1; \
	done && for tape in promo/*.tape; do \
		uv run vhs "$$tape" || exit 1; \
	done
	uv run --group accessibility python demo/screenshots.py --promo

schema:
	uv run veridelta schema > docs/schema/veridelta.schema.json
	for output in run validate crosswalk suggest baseline error; do \
		uv run veridelta schema $$output > docs/schema/$$output.schema.json; \
	done

docs:
	uv run mkdocs build --strict

docs-serve:
	uv run mkdocs serve

all: format lint hooks test docs

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	rm -rf site/ dist/ build/
	rm -f .coverage coverage.xml
	find . -type d -name "__pycache__" -exec rm -rf {} +