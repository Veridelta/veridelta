# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the machine the unit suite runs on out of what its tests see."""

import os

import pytest

from veridelta import mcp_server


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
