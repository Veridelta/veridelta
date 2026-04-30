# Security Policy

Security is a core priority for the Veridelta framework. We take vulnerabilities in our ingestion engine, configuration parser, and dependency chain seriously.

## Supported Versions

We provide security updates for the **current minor release series**. Because Veridelta is currently in pre-1.0 development (`0.x.y`), only the latest minor version receives active security patches.

| Version | Supported          |
| ------- | ------------------ |
| Latest Minor (Current) | :white_check_mark: |
| Older Minors           | :x:                |

## Scope

Veridelta is a local CLI and Python library. The following are considered in-scope security vulnerabilities:
* **Arbitrary Code Execution (ACE):** Vulnerabilities allowing execution of malicious code via crafted `veridelta.yaml` configurations.
* **Path Traversal:** Ability to read or write files outside the intended execution directories during dataset caching or artifact generation.
* **Dependency Vulnerabilities:** Exploitable CVEs in core dependencies (e.g., `polars`, `pydantic`, `pyyaml`).

**Out of Scope:**
* Misconfigurations intentionally authored by the user (e.g., explicitly ignoring a critical column).
* Denial of Service (DoS) caused by loading datasets larger than the host machine's available RAM.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

If you discover a potential security vulnerability in Veridelta, please email the core maintainer team directly at **veridelta.labs@gmail.com**. 

### Our Disclosure Process
1. **Acknowledgement:** We will acknowledge receipt of your email within **48 hours**.
2. **Triage & Fix:** We will provide a timeline for triage. If confirmed, we will develop a patch in a private fork.
3. **Release:** We will cut a new patch release and publish a coordinated GitHub Security Advisory.
4. **Credit:** You will be fully credited for the discovery in the release notes and advisory (unless you prefer to remain anonymous).