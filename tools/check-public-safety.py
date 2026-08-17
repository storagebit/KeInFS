#!/usr/bin/env python3
"""Reject operator data, real infrastructure identifiers, and secrets.

The checker scans the complete Git index for commits and the complete tracked
working tree in CI. Reports contain only fingerprints and locations; a failed
security check must not repeat the value it is trying to keep private.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit


DOCUMENTATION_V4 = tuple(
    ipaddress.ip_network(value)
    for value in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)
DOCUMENTATION_V6 = ipaddress.ip_network("2001:db8::/32")
SPECIAL_V4 = {"0.0.0.0", "255.255.255.0", "255.255.255.255"}
RESOURCE_URI_SCHEMES = {"keinfs", "s3"}

# Exact public hosts intentionally referenced by source, dependency metadata,
# install scripts, or citations. Operational examples belong under .example.
PUBLIC_HOSTS = {
    "apt.grafana.com",
    "aws.amazon.com",
    "cloud-images.ubuntu.com",
    "crates.io",
    "docs.jarvislabs.ai",
    "docs.rs",
    "github.com",
    "hooks.slack.com",
    "intuitionlabs.ai",
    "kernel.dk",
    "modelcontextprotocol.io",
    "nebius.com",
    "no-color.org",
    "opencollective.com",
    "registry.npmjs.org",
    "rocksdb.org",
    "sh.rustup.rs",
    "studio.westerndigital.com",
    "www.gmicloud.ai",
}

# Public documentation inventory. A new Markdown path requires an explicit
# review and a deliberate hash addition; arbitrary work notes are rejected.
ALLOWED_MARKDOWN_PATH_HASHES = {
    "1ef367fd85419604ce8c95a1cc5fd61d840361eadacc2929f8c6b5ff66c2f196",
    "2757092dfa05db4b507972614261a03abd88cca9aa12ad6e9576eef55578a494",
    "3a14af19b465f492b03722af63159229b9dcb5782b6ad64bf9fd5489ba781393",
    "40385cb6628758761d7574c9f20eba1d158007a3517eb3f2f47f32ad61ae41b1",
    "45803bbea96303daa757806781f1d6a92dbba3a20064c6cb94f6a33f272c7c3b",
    "5a831ea67cf5cf8703b0de46901ab25bd191f56b320053be9332d9a3b0d01d15",
    "71513e5b3d2ee43352bea32faa8d16584c84bfb18f91b3c20fc623d97c7a41f1",
    "798b2fd291f9b2c36debffe0afd06edac0eba382486b14a0dd893cd41d8f073c",
    "7ba8b8a8e23a5061c65994d59f6f68e14d735440ac5374fd7eb2070f136ba127",
    "818fb076ad8a68f40bd910a528333ebf080f683492a0a8780e36dffc6f255ce9",
    "81e41d218eb79118f24d020aff9a5ae2fecdf59913e2891aaa3114a7e762d823",
    "a6804e2078dc054e5858de473282da80b79856ab4ca2da65c05d32b05231cbae",
    "af32553fa923bc6c511e4a7d8d5ec44f89746e27ac8f5f029dd00624edac39c7",
    "b8ac714c7be33a48ebe4e7090f581799bc3f6a508f4247f03ecc0235e192facb",
    "c88e7884ed5e186252fb9374259d99cbb54d32ce6859a82f87ed79841f464e82",
    "da42780575c07c6164ac66be746e09f2c03d9fe74a4b654e5b5c1f9c5b9aace5",
    "db5a3dede6661488bb0cfe3d4a94c194f53b05391d1e2185a75270147eebe2d9",
    "dea34feba61661892bc37366556c1b9a39c2d8d078fa6d05cd34096600cdee8a",
    "e01e5f2db3ddaeca428e61a800ce604cf1718d8fcac136327883b6d393455e1e",
    "e335e4369a81704d192dd4266007a7453fc38369e07978244f603f382c11996b",
    "f8b958497ad5d2bf74ae6d3d579c2f40a33b85dadb52327a2b5636c3132890af",
    "faf9c2ea758bc5659d24d27458c2b80f17dbe7e987f2165db79c20e581937e69",
}
FORBIDDEN_DIRS = {"build", "coverage", "dist", "node_modules", "target"}
FORBIDDEN_SUFFIXES = {
    ".7z",
    ".db",
    ".dump",
    ".gz",
    ".har",
    ".jks",
    ".key",
    ".log",
    ".p12",
    ".pcap",
    ".pcapng",
    ".pem",
    ".pfx",
    ".rar",
    ".sqlite",
    ".tar",
    ".tfstate",
    ".tgz",
    ".zip",
}

IPV4_RE = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
IPV6_CANDIDATE_RE = re.compile(
    r"(?<![A-Za-z0-9_.:])(?:[0-9A-Fa-f]{0,4}:){2,}[0-9A-Fa-f:.]*(?![A-Za-z0-9_.:])"
)
URL_RE = re.compile(r"(?i)\b[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"'`]+")
EMAIL_RE = re.compile(r"(?<![\w.+-])[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}")
INTERNAL_NAME_RE = re.compile(r"(?i)\b(?:[A-Za-z0-9-]+\.)+(?:corp|home|internal|lan|local)\b")
USER_HOME_RE = re.compile(r"/(?:Users|home)/[A-Za-z0-9._-]+(?:/|\b)")
WINDOWS_HOME_RE = re.compile(r"(?i)\b[A-Z]:\\Users\\[^\\\s]+(?:\\|\b)")
SCP_DESTINATION_RE = re.compile(
    r"(?<![A-Za-z0-9_.+-])([A-Za-z0-9._-]+)@([A-Za-z0-9.-]+):(?=[^\s])"
)
HOST_ASSIGNMENT_RE = re.compile(
    r'''(?ix)\b["']?(?:auth_server|host|host_name|hostname|target_host)["']?'''
    r'''\s*[:=]\s*["']([^"']+)["']'''
)
PARTIAL_PRIVATE_V4_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9.])"
    r"(?:"
    r"(?:10|192\.168|172\.(?:1[6-9]|2[0-9]|3[01]))"
    r"(?:\.[0-9]{1,3}){0,2}\.(?:x|\*)"
    r"|(?:10(?:\.[0-9]{1,3}){1,2}"
    r"|(?:192\.168|172\.(?:1[6-9]|2[0-9]|3[01]))(?:\.[0-9]{1,3}){0,2})"
    r"/[0-9]{1,2}"
    r"|10(?:\.[0-9]{1,3}){2}\."
    r"|(?:192\.168|172\.(?:1[6-9]|2[0-9]|3[01]))\.[0-9]{1,3}\."
    r")"
    r"(?![A-Za-z0-9.*])"
)
PRIVATE_KEY_RE = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")
PROVIDER_TOKEN_RES = (
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bASIA[0-9A-Z]{16}\b"),
    re.compile(r"\bgh(?:p|o|u|s|r)_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bxox(?:b|a|p|r|s)-[A-Za-z0-9-]{10,}\b"),
)
CLOUD_ID_RE = re.compile(r"\bocid1\.[a-z0-9.]+\b", re.IGNORECASE)
FDB_CLUSTER_RE = re.compile(r"\b[A-Za-z0-9_-]+:[A-Za-z0-9_-]+@[A-Za-z0-9.:-]+:[0-9]{2,5}\b")
CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:access_key|api_key|password|passwd|private_key|secret(?:_key)?|token)\b"
    r"\s*[:=]\s*[\"']([^\"']+)[\"']"
)
UNQUOTED_CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?im)^\s*(?:access_key|api_key|password|passwd|private_key|secret(?:_key)?|token)"
    r"\s*[:=]\s*(?![\"'])([^\s#,}\]]+)"
)
BEARER_TOKEN_RE = re.compile(r"(?i)\bbearer\s+([A-Za-z0-9._~+/=-]{12,})")
JWT_RE = re.compile(
    r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{7,}\.[A-Za-z0-9_-]{10,}\."
    r"[A-Za-z0-9_-]{10,}(?![A-Za-z0-9_-])"
)
SENSITIVE_QUERY_KEYS = {
    "access_key",
    "api_key",
    "credential",
    "key",
    "password",
    "secret",
    "sig",
    "signature",
    "token",
}


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    category: str
    fingerprint: str


def digest(value: str) -> str:
    return hashlib.sha256(value.lower().encode("utf-8")).hexdigest()


def fingerprint(value: str) -> str:
    return digest(value)[:12]


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def git_bytes(*args: str) -> bytes:
    return subprocess.check_output(("git", *args), stderr=subprocess.DEVNULL)


def tracked_paths() -> list[str]:
    return [item.decode("utf-8") for item in git_bytes("ls-files", "-z", "--cached").split(b"\0") if item]


def read_blob(path: str, mode: str) -> bytes:
    if mode == "index":
        return git_bytes("show", f":{path}")
    return Path(path).read_bytes()


def path_findings(path: str) -> list[Finding]:
    result: list[Finding] = []
    item = Path(path)
    basename = item.name
    parts = set(item.parts)
    category = None
    if basename.lower().startswith(".") and basename.lower().endswith("ignore"):
        category = "unapproved_ignore_file"
    elif item.suffix.lower() == ".md" and digest(path) not in ALLOWED_MARKDOWN_PATH_HASHES:
        category = "unapproved_markdown_path"
    elif basename == ".env" or (basename.startswith(".env.") and not basename.endswith(".example")):
        category = "environment_file"
    elif parts & FORBIDDEN_DIRS:
        category = "generated_directory"
    elif item.suffix.lower() in FORBIDDEN_SUFFIXES:
        category = "sensitive_or_generated_file"
    if category:
        result.append(Finding(path, 1, category, fingerprint(path)))
    return result


def allowed_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return True
    if address.version == 4:
        return (
            value in SPECIAL_V4
            or address.is_loopback
            or any(address in network for network in DOCUMENTATION_V4)
        )
    return address.is_loopback or address.is_unspecified or address in DOCUMENTATION_V6


def allowed_host(host: str | None) -> bool:
    if not host:
        return False
    value = host.rstrip(".").lower()
    try:
        ipaddress.ip_address(value)
    except ValueError:
        pass
    else:
        return allowed_ip(value)
    if value == "localhost" or value.endswith((".example", ".invalid")):
        return True
    return value in PUBLIC_HOSTS


def placeholder_credential(value: str) -> bool:
    lowered = value.lower()
    return (
        (value.startswith("<") and value.endswith(">"))
        or "${" in value
        or "redacted" in lowered
        or value.endswith("...")
    )


def content_findings(path: str, data: bytes) -> list[Finding]:
    if b"\0" in data:
        return [Finding(path, 1, "binary_blob", fingerprint(path))]

    text = data.decode("utf-8", errors="replace")
    result: set[Finding] = set()

    def add(category: str, value: str, offset: int) -> None:
        result.add(Finding(path, line_number(text, offset), category, fingerprint(value)))

    for match in IPV4_RE.finditer(text):
        value = match.group()
        try:
            ipaddress.ip_address(value)
        except ValueError:
            continue
        if not allowed_ip(value):
            add("non_documentation_ipv4", value, match.start())

    for match in IPV6_CANDIDATE_RE.finditer(text):
        value = match.group().strip("[](){}<>,;")
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            continue
        if address.version == 6 and not allowed_ip(value):
            add("non_documentation_ipv6", value, match.start())

    for match in PARTIAL_PRIVATE_V4_RE.finditer(text):
        add("partial_private_network", match.group(), match.start())

    for match in URL_RE.finditer(text):
        raw = match.group().rstrip("),.;]}")
        if raw.endswith("://") or any(marker in raw for marker in ("${", "$(", "{{", "{", "<")):
            continue
        try:
            parsed = urlsplit(raw)
            host = parsed.hostname
        except ValueError:
            add("malformed_url_authority", raw, match.start())
            continue
        if parsed.username is not None or parsed.password is not None:
            add("embedded_url_userinfo", raw, match.start())
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() in SENSITIVE_QUERY_KEYS and value and not placeholder_credential(value):
                add("sensitive_url_query", value, match.start())
        if host and host.lower() == "hooks.slack.com":
            path_parts = [part for part in parsed.path.split("/") if part]
            if len(path_parts) >= 4 and not any(
                marker in raw.lower()
                for marker in ("example", "placeholder", "redacted", "your-")
            ):
                add("embedded_webhook_credential", raw, match.start())
        if parsed.scheme.lower() in RESOURCE_URI_SCHEMES:
            continue
        if parsed.scheme.lower() in {"file", "unix"} and not host:
            continue
        if not allowed_host(host):
            add("unapproved_url_host", host or raw, match.start())

    for match in SCP_DESTINATION_RE.finditer(text):
        host = match.group(2)
        if not allowed_host(host):
            add("unapproved_scp_host", host, match.start(2))

    for match in HOST_ASSIGNMENT_RE.finditer(text):
        value = match.group(1)
        if any(marker in value for marker in ("${", "$(", "{{", "{", "<")):
            continue
        try:
            host = urlsplit(f"//{value}").hostname
        except ValueError:
            host = None
        if not allowed_host(host):
            add("unapproved_hostname_assignment", value, match.start(1))

    for match in INTERNAL_NAME_RE.finditer(text):
        add("internal_hostname", match.group(), match.start())

    for match in EMAIL_RE.finditer(text):
        value = match.group()
        domain = value.rsplit("@", 1)[1].lower()
        if not (
            domain in {"example.com", "example.org", "example.net", "users.noreply.github.com"}
            or domain.endswith(".example")
        ):
            add("non_example_email", value, match.start())

    for match in USER_HOME_RE.finditer(text):
        add("personal_home_path", match.group(), match.start())

    for match in WINDOWS_HOME_RE.finditer(text):
        add("windows_personal_home_path", match.group(), match.start())

    if PRIVATE_KEY_RE.search(text):
        match = PRIVATE_KEY_RE.search(text)
        assert match is not None
        add("private_key", match.group(), match.start())

    for pattern in PROVIDER_TOKEN_RES:
        for match in pattern.finditer(text):
            add("provider_token", match.group(), match.start())

    for match in CLOUD_ID_RE.finditer(text):
        add("cloud_resource_id", match.group(), match.start())

    for match in FDB_CLUSTER_RE.finditer(text):
        add("embedded_fdb_cluster_credential", match.group(), match.start())

    for match in CREDENTIAL_ASSIGNMENT_RE.finditer(text):
        value = match.group(1)
        if not placeholder_credential(value):
            add("credential_literal", value, match.start(1))

    for match in UNQUOTED_CREDENTIAL_ASSIGNMENT_RE.finditer(text):
        value = match.group(1)
        if not placeholder_credential(value):
            add("unquoted_credential_literal", value, match.start(1))

    for match in BEARER_TOKEN_RE.finditer(text):
        value = match.group(1)
        if not placeholder_credential(value):
            add("bearer_token", value, match.start(1))

    for match in JWT_RE.finditer(text):
        add("jwt", match.group(), match.start())

    return sorted(result)


def self_test() -> int:
    # Assemble unsafe specimens from fragments so this checker never publishes
    # a reusable secret or a literal private-environment value in its own source.
    unsafe_cases = {
        "bearer_token": "Bearer" + " " + "abcdefghijklmnop",
        "embedded_fdb_cluster_credential": (
            "barnacle:" + "short" + "@" + "192.0.2.1:4500"
        ),
        "embedded_url_userinfo": (
            "https://" + "deckhand:password" + "@" + "github.com/example"
        ),
        "embedded_webhook_credential": (
            "https://" + "hooks.slack.com/services/T111/B222/secretvalue"
        ),
        "jwt": (
            "eyJ" + "hbGciOiJIUzI1NiJ9" + "."
            + "eyJzdWIiOiIxMjM0NTY3ODkwIn0" + "."
            + "abcdefghijklmnop"
        ),
        "non_documentation_ipv4": "10" + ".23.45.67",
        "partial_private_network": "192" + ".168.42.*",
        "sensitive_url_query": (
            "https://" + "github.com/example?token=definitely-secret"
        ),
        "unapproved_hostname_assignment": (
            "host" + " = \"" + "boring-host" + "\""
        ),
        "unapproved_scp_host": "scp file deckhand@" + "boring-host:/tmp",
        "unquoted_credential_literal": "token" + ": " + "definitely-secret",
        "windows_personal_home_path": "C:" + "\\Users\\deckhand\\secrets.txt",
    }
    for expected, specimen in unsafe_cases.items():
        categories = {
            finding.category
            for finding in content_findings("self-test.txt", specimen.encode("utf-8"))
        }
        if expected not in categories:
            print(f"public-safety self-test: missing detector {expected}", file=sys.stderr)
            return 1

    unsafe_paths = {
        "unapproved_ignore_file": ".sample" + "ignore",
        "unapproved_markdown_path": "unlisted" + ".md",
    }
    for expected, specimen in unsafe_paths.items():
        categories = {finding.category for finding in path_findings(specimen)}
        if expected not in categories:
            print(f"public-safety self-test: missing path detector {expected}", file=sys.stderr)
            return 1

    safe_cases = (
        "http://captain-chunkbeard.example:8080",
        "http://192.0.2.44:8080",
        "https://github.com/storagebit/KeInFS",
        "https://hooks.slack.com/services/placeholder/example",
        "s3://bucket-buccaneer.example/model.bin",
        "Section 10.5 and Figure 10.**",
        "placement.target_identifier.task_identifier",
    )
    for specimen in safe_cases:
        findings = content_findings("self-test.txt", specimen.encode("utf-8"))
        if findings:
            categories = ",".join(sorted({finding.category for finding in findings}))
            print(
                f"public-safety self-test: safe specimen blocked by {categories}",
                file=sys.stderr,
            )
            return 1

    print("public-safety self-test: pass")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--index", action="store_true", help="scan complete staged Git index")
    source.add_argument("--worktree", action="store_true", help="scan complete tracked working tree")
    source.add_argument("--self-test", action="store_true", help="exercise positive and negative detectors")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    mode = "index" if args.index else "worktree"

    findings: list[Finding] = []
    paths = tracked_paths()
    for path in paths:
        findings.extend(path_findings(path))
        try:
            data = read_blob(path, mode)
        except (OSError, subprocess.CalledProcessError):
            findings.append(Finding(path, 1, "unreadable_tracked_file", fingerprint(path)))
            continue
        findings.extend(content_findings(path, data))

    findings = sorted(set(findings))
    if findings:
        print("public-safety: BLOCKED; private or unsafe tracked content detected", file=sys.stderr)
        for finding in findings:
            print(
                f"{finding.path}:{finding.line}: {finding.category} "
                f"fingerprint={finding.fingerprint}",
                file=sys.stderr,
            )
        print(f"public-safety: {len(findings)} finding(s); literal values withheld", file=sys.stderr)
        return 1

    print(f"public-safety: clean ({len(paths)} tracked files scanned from {mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
