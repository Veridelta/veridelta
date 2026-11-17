---
type: Decision
title: The lockfile covers only the Pythons CI tests, and not Emscripten
description: Why `[tool.uv] environments` stops the lockfile below Python 3.15 and leaves out Emscripten, chosen over a marker on connectorx or an upper bound on requires-python.
status: stable
decided: 2026-11-17
generated: { by: claude-code, at: 2026-11-17T18:16:37Z }
---

**Claim:** `[tool.uv] environments` in `pyproject.toml` limits `uv.lock` to Python 3.11.5 to 3.14, the releases the classifiers name and CI tests, on every platform but Emscripten. The package's own metadata is unchanged: `requires-python` stays `>=3.11`, and every extra names the same requirements.

**Evidence:** uv 0.12.23, which CI and the security-update workflow run, writes lock revision 5. It refuses to relock when a package that ships wheels alone has none for a Python and platform split the lockfile covers, and uv 0.13.9 does the same. ConnectorX 0.4.6 and 0.4.7a1 ship wheels for CPython 3.10 to 3.14 on Linux, macOS, and Windows, and none for 3.15 or for Emscripten. On main, `uvx --from uv==0.12.23 uv lock --upgrade-package connectorx` fails for the split `python_full_version >= '3.15' and sys_platform == 'win32'`. Bounding the Python range alone lets one relock through, but the next one, from the revision 5 lock it wrote, fails for the split `python_full_version == '3.14.*' and sys_platform == 'emscripten'`, with uv 0.12.23 and 0.13.9 alike. Leaving out Emscripten alone fails again at 3.15. With both limits, four relocks in a row and a full `uv lock --upgrade` succeed with both releases. The lock then drops only `httpx2-jsfetch`, which installs on Emscripten alone, and changes no other version. `tests/unit/test_lockfile.py` holds the lock to the classifiers and checks that each wheel-only requirement has a wheel for every Python and platform the lock covers. `make install` on Python 3.15 failed on main too, since ConnectorX has no wheel to install there.

**Alternative considered:** The marker `python_version < '3.15'` on `connectorx` in the `database` extra, so the lock needs no ConnectorX for Python 3.15.

**Why rejected:** The relock still fails, on `psycopg-binary` from the dev group in the Python 3.15 Emscripten split, so every wheel-only package would need its own marker. The marker also reaches users: on Python 3.15, `pip install veridelta[database]` would succeed without ConnectorX and fail only at the first read. An upper bound on `requires-python` would refuse the whole package on Python 3.15 for everyone, though only one extra lacks a wheel there.

**How to reverse:** When ConnectorX ships Python 3.15 wheels and CI tests 3.15, add 3.15 to the classifiers and the CI matrix, raise the bound in `environments` to `'3.16'`, and run `uv lock`. `tests/unit/test_lockfile.py` fails until all three agree, and names any wheel-only requirement that still lacks a wheel. To cover Emscripten again, drop `sys_platform != 'emscripten'` once every wheel-only requirement ships an Emscripten wheel.
