---
type: Decision
title: The MCP server reads an Azure Iceberg table only with an account key or a SAS token
description: Why the MCP server's Iceberg FileIO refuses a Microsoft Entra ID sign-in and a key that is not base64, chosen over checking that a client secret is set.
status: stable
decided: 2026-11-16
generated: { by: claude-code, at: 2026-11-16T19:00:00+00:00 }
---

**Claim:** On the MCP server, `checked_file_io` in `src/veridelta/connectors/lakehouse.py` builds PyArrow's `AzureFileSystem` only with an `adls.account-key` in base64's padded shape, `BASE64`, or a non-blank `adls.sas-token`, and refuses `adls.client-id`, `adls.client-secret`, and `adls.tenant-id` with a `ConfigError`. `veridelta run` keeps every credential PyIceberg accepts.

**Evidence:** PyArrow 23 calls `std::terminate` when its Azure client throws while it signs a request. Through the server's `check_configuration`, a client-secret trio that Microsoft Entra ID rejects ended the process with exit code 134 on `Azure::Core::Credentials::AuthenticationException`, and `adls.account-key: 'not-base64!'` ended it on `Unexpected end of Base64 encoded string`, while a wrong key in base64 and a wrong SAS token raised `OSError`. Each of 9,594 keys over `A/+=` up to eight characters in `BASE64`'s shape signed a request without ending the process, and `BASE64` accepts the same keys as Python 3.14's strict `binascii.a2b_base64`, which accepts extra padding on Python 3.11. `test_it_builds_no_azure_file_system_that_can_end_the_process` and `test_it_gives_pyarrow_only_an_account_key_it_can_decode` in `tests/integration/test_lakehouse_sources.py` pin the refusals and the keys passed on. An `adls.account-name` with a port or brackets in it, such as `acct:99` or `[::1]`, ended the process too, with a valid key or SAS token, as PyArrow writes the name into the host it reaches, so `_AZURE_ACCOUNT` admits only a storage account's name, and `test_it_builds_no_azure_file_system_for_a_name_that_is_no_account` pins that.

**Alternative considered:** Accept a client secret when `adls.client-id`, `adls.client-secret`, and `adls.tenant-id` are all set, as PyIceberg does.

**Why rejected:** A wrong or expired client secret, or an outage at the sign-in host, is an ordinary mistake that one tool call makes, and it would end the server for every client. The sign-in also reaches a host that no `--allow-host` names, and PyArrow gives the FileIO no way to fetch the token itself.

**How to reverse:** Remove the `_AZURE_SIGN_INS` check from `_refuse_azure_options` in `src/veridelta/connectors/lakehouse.py` once PyArrow raises instead of aborting on a failed sign-in, and change the matching cases in `test_it_builds_no_azure_file_system_that_can_end_the_process` and the sentences in `docs/agents.md` and `docs/upgrading.md`.
