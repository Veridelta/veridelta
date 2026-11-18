# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the machine the unit suite runs on out of what its tests see."""

import os

import pytest

from veridelta import mcp_server


@pytest.fixture(autouse=True)
def _no_url_logins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset each variable that holds a URL login, whatever its name.

    The MCP server masks a URL login from any variable in every answer, so a
    variable on the machine the suite runs on, such as a Git setting in
    `GIT_CONFIG_VALUE_2` or a proxy with a login on a CI runner, would change
    what a test sees. A proxy whose login no client can parse would stop the
    server from starting. Git refuses to run when a `GIT_CONFIG_VALUE_n` that
    `GIT_CONFIG_COUNT` counts is unset, so the count goes too, and Git then
    reads none of those settings.
    """
    for name, value in list(os.environ.items()):
        if mcp_server._url_login_values(name, value) or mcp_server._cut_proxy_login(name, value):
            monkeypatch.delenv(name)
            if name.upper().startswith("GIT_CONFIG_VALUE_"):
                monkeypatch.delenv("GIT_CONFIG_COUNT", raising=False)


@pytest.fixture(autouse=True)
def _no_reader_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset each credential variable a reader takes from the MCP server's environment, in any spelling.

    The server masks each such value in every answer, and refuses to start on
    one it cannot mask, so a value from the machine the suite runs on, such as
    a dummy a shell exports, would change what a test that builds it sees.
    """
    left_out = {
        *mcp_server._ENVIRONMENT_SLOTS,
        *mcp_server._TOKEN_FILE_VARIABLES,
        mcp_server._KEY_JSON_VARIABLE,
    }
    for name in list(os.environ):
        if mcp_server._name_as_read(name) in left_out:
            monkeypatch.delenv(name)
