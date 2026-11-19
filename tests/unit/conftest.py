# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the machine the unit suite runs on out of what its tests see."""

import importlib.util
import os
from pathlib import Path

import pytest

from veridelta import mcp_server


@pytest.fixture(autouse=True)
def _no_reader_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Unset each credential, endpoint, region, account, and folder a reader takes from the MCP server's environment.

    The server masks each credential in every answer, and refuses to start on
    one it cannot mask. It also refuses a side whose reader would take an
    endpoint, a region, an account, or a Hadoop folder from its environment
    that is not allowed, including the endpoint or the region of an AWS
    profile. So a
    value from the machine the suite runs on, such as a dummy a shell exports
    or a local MinIO in `~/.aws/config`, would change what a test that builds
    it sees. A name counts in any spelling. AWS_CONFIG_FILE names a file that
    does not exist, so no profile is read, and botocore reads no
    `~/.aws/models`.
    """
    left_out = {
        *mcp_server._ENVIRONMENT_SLOTS,
        *mcp_server._TOKEN_FILE_VARIABLES,
        mcp_server._KEY_JSON_VARIABLE,
    }
    endpoints = {
        *mcp_server._ENVIRONMENT_ENDPOINTS,
        mcp_server._EMULATOR_SWITCH,
        *mcp_server._CERTIFICATE_SWITCHES,
        *mcp_server._SERVICE_ACCOUNT_FILES,
        *mcp_server._HADOOP_CONFIGURATION,
        *mcp_server._ENVIRONMENT_REGIONS,
        mcp_server._ACCOUNT_VARIABLE,
        mcp_server._UNIVERSE_VARIABLE,
        mcp_server._MONITORING_SWITCH,
        "AWS_DATA_PATH",
    }
    for name in list(os.environ):
        if (
            mcp_server._name_as_read(name) in left_out
            or name.upper() in endpoints
            or mcp_server._motherduck_setting(name)
        ):
            monkeypatch.delenv(name)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))
    if importlib.util.find_spec("botocore") is not None:
        import botocore.loaders  # pyright: ignore[reportMissingTypeStubs] - botocore ships no types

        monkeypatch.setattr(
            botocore.loaders.Loader, "CUSTOMER_DATA_PATH", str(tmp_path / "no-aws-models")
        )
