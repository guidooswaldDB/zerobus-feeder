#!/usr/bin/env python3
"""Send a configurable synthetic data stream to a Databricks Zerobus endpoint.

Auth for ingestion: OAuth2 client-credentials via a Databricks service principal
(the Zerobus SDK handles token acquisition). A Databricks CLI profile is only
needed for the optional --create-sp and --create-table helpers.
"""

from __future__ import annotations

import argparse
import base64
import configparser
import getpass
import json
import logging
import os
import random
import re
import signal
import string
import sys
import threading
import time
import traceback
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text


SCRIPT_DIR = Path(__file__).resolve().parent
LAST_VALUES_FILE = SCRIPT_DIR / ".zerobus_feeder_last.yaml"
LOG_FILE = SCRIPT_DIR / "zerobus_feeder.log"
DATABRICKS_CFG = Path.home() / ".databrickscfg"

CLOUDS = ("aws", "azure", "gcp")
SUPPORTED_TYPES = {
    "string", "int", "long", "float", "double",
    "boolean", "timestamp", "date", "binary",
}
DELTA_TYPE_MAP = {
    "string": "STRING", "int": "INT", "long": "BIGINT",
    "float": "FLOAT", "double": "DOUBLE", "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP", "date": "DATE", "binary": "BINARY",
}

console = Console()
logger = logging.getLogger("zerobus_feeder")


# ---------------------------------------------------------------- Logging --

def setup_logging() -> None:
    """Attach a file handler that captures every step and output.

    Runs once per process; subsequent calls are no-ops. The log file is
    appended to across runs with a clear session banner.
    """
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    except Exception as e:
        console.print(f"[yellow]Could not open log file {LOG_FILE}: {e}[/yellow]")
        return
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.info("=" * 72)
    logger.info("Session started  pid=%d  cwd=%s  python=%s",
                os.getpid(), os.getcwd(), sys.version.split()[0])
    logger.info("argv=%s", " ".join(sys.argv))


def _strip_markup(s: str) -> str:
    """Convert a Rich-markup string to plain text for log output."""
    try:
        return Text.from_markup(str(s)).plain
    except Exception:
        return str(s)


def say(message: str, level: int = logging.INFO) -> None:
    """Print to the console (with Rich markup) and mirror to the log file."""
    console.print(message)
    logger.log(level, _strip_markup(message))


def _mask(secret: str, show_last: int = 4) -> str:
    if not secret:
        return ""
    if len(secret) <= show_last:
        return "*" * len(secret)
    return "*" * (len(secret) - show_last) + secret[-show_last:]


def log_config(cfg: "Config", label: str) -> None:
    safe = asdict(cfg)
    if safe.get("client_secret"):
        safe["client_secret"] = _mask(safe["client_secret"])
    logger.info("%s: %s", label, safe)


# ---------------------------------------------------------------- Config ----

@dataclass
class Config:
    qps: float = 10.0
    schema_file: str = ""
    workspace_id: str = ""
    region: str = ""
    cloud: str = ""  # aws | azure | gcp
    table_name: str = ""
    client_id: str = ""
    client_secret: str = ""
    profile: str = ""
    workspace_url: str = ""
    warehouse_id: str = ""
    sp_display_name: str = "zerobus-feeder"

    def zerobus_endpoint(self) -> str:
        cloud = (self.cloud or "").lower()
        if cloud == "aws":
            return f"{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com"
        if cloud == "azure":
            return f"{self.workspace_id}.zerobus.{self.region}.azuredatabricks.net"
        if cloud == "gcp":
            return f"{self.workspace_id}.zerobus.{self.region}.gcp.databricks.com"
        raise ValueError(f"Unsupported cloud: {self.cloud!r}")


# (field, label, is_secret, is_required)
PARAMS: list[tuple[str, str, bool, bool]] = [
    ("qps",           "Transmission rate (QPS)",               False, True),
    ("schema_file",   "Data structure JSON path",              False, True),
    ("workspace_id",  "Workspace ID (digits)",                 False, True),
    ("region",        "Region (e.g. us-west-2, eastus)",       False, True),
    ("cloud",         "Cloud (aws | azure | gcp)",             False, True),
    ("table_name",    "Full table name (catalog.schema.table)", False, True),
    ("client_id",     "Service Principal Client ID",           False, True),
    ("client_secret", "Service Principal Client Secret",       True,  True),
    ("workspace_url", "Workspace URL (https://...)",           False, True),
    ("profile",       "Databricks CLI profile (optional)",     False, False),
    ("warehouse_id",  "SQL Warehouse ID (for --create-table)", False, False),
    ("sp_display_name", "Service Principal display name (for --create-sp)", False, False),
]


# ---------------------------------------------------------- Persistence ----

def load_last_values() -> dict:
    if not LAST_VALUES_FILE.exists():
        return {}
    try:
        with LAST_VALUES_FILE.open() as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        console.print(f"[yellow]Could not read {LAST_VALUES_FILE.name}: {e}[/yellow]")
        return {}


def save_last_values(cfg: Config) -> None:
    try:
        with LAST_VALUES_FILE.open("w") as f:
            yaml.safe_dump(asdict(cfg), f, sort_keys=False)
    except Exception as e:
        console.print(f"[yellow]Could not save {LAST_VALUES_FILE.name}: {e}[/yellow]")


def load_yaml_config(path: str) -> dict:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML config must be a mapping, got {type(data).__name__}")
    valid = {f.name for f in fields(Config)}
    unknown = set(data) - valid
    if unknown:
        console.print(f"[yellow]Ignoring unknown YAML keys: {sorted(unknown)}[/yellow]")
    return {k: v for k, v in data.items() if k in valid}


def offer_config_yaml_copy(cfg: "Config") -> None:
    """Prompt the user to dump the current config (including just-created SP
    credentials) into a reusable YAML file for `--config`.
    """
    if not Confirm.ask(
        "Copy the current config (including new SP credentials) to a YAML file for reuse with --config?",
        default=False,
    ):
        return
    default_path = str(SCRIPT_DIR / "feeder_config.yaml")
    raw = Prompt.ask("YAML file path", default=default_path)
    p = Path(raw).expanduser()
    if p.exists() and not Confirm.ask(f"{p} already exists — overwrite?", default=False):
        say("[yellow]Skipped writing YAML config.[/yellow]", level=logging.WARNING)
        return
    try:
        with p.open("w") as f:
            f.write(
                "# Zerobus Feeder config — generated by the first-run wizard.\n"
                "# WARNING: contains client_secret. Do not commit this file.\n"
            )
            yaml.safe_dump(asdict(cfg), f, sort_keys=False)
        say(f"[green]✓[/green] Wrote config to {p}")
        logger.info("wrote YAML config to %s", p)
        say(
            f"[yellow]{p.name} contains your client_secret — add it to .gitignore "
            f"if this is a git repo.[/yellow]",
            level=logging.WARNING,
        )
    except Exception as e:
        logger.exception("write YAML config failed")
        say(f"[red]Could not write {p}: {e}[/red]", level=logging.ERROR)


# ----------------------------------------------------- CLI profile helpers ----

def list_cli_profiles() -> list[str]:
    if not DATABRICKS_CFG.exists():
        return []
    # default_section=None would be cleanest but is only honoured on write.
    # configparser always strips [DEFAULT] from sections(); re-add it when it
    # looks like a real profile (has a host) so the user sees every profile.
    cp = configparser.ConfigParser()
    try:
        cp.read(DATABRICKS_CFG)
    except Exception:
        return []
    profiles: list[str] = []
    if cp.defaults().get("host"):
        profiles.append("DEFAULT")
    profiles.extend(cp.sections())
    return profiles


def _read_profile_raw(profile: str) -> dict:
    """Read a single profile from ~/.databrickscfg without touching the SDK
    (so unauthenticated profiles don't raise auth errors just for lookup).

    configparser inherits [DEFAULT] values into every section, which would
    silently give unrelated profiles a DEFAULT workspace_id. We parse the file
    manually so each section only returns keys it explicitly lists.
    """
    if not DATABRICKS_CFG.exists():
        return {}
    sections: dict[str, dict] = {}
    current: Optional[str] = None
    try:
        with DATABRICKS_CFG.open() as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or s.startswith(";"):
                    continue
                if s.startswith("[") and s.endswith("]"):
                    current = s[1:-1].strip()
                    sections.setdefault(current, {})
                    continue
                if current and "=" in s:
                    k, _, v = s.partition("=")
                    sections[current][k.strip()] = v.strip()
    except Exception:
        return {}
    return sections.get(profile, {})


def _workspace_id_from_host(host: str) -> Optional[str]:
    """Azure workspace URLs embed the workspace id: adb-<id>.<n>.azuredatabricks.net."""
    m = re.search(r"adb-(\d+)\.", host or "")
    return m.group(1) if m else None


def enrich_from_profile(cfg: Config) -> None:
    if not cfg.profile:
        return
    logger.info("enriching from profile '%s'", cfg.profile)
    raw = _read_profile_raw(cfg.profile)
    logger.info("profile raw keys: %s", sorted(raw.keys()))
    if not cfg.workspace_url and raw.get("host"):
        cfg.workspace_url = raw["host"].rstrip("/")
        say(f"[dim]Prefilled workspace_url from profile: {cfg.workspace_url}[/dim]")
    if not cfg.workspace_id:
        wid = raw.get("workspace_id") or _workspace_id_from_host(raw.get("host", ""))
        if wid:
            cfg.workspace_id = str(wid)
            say(f"[dim]Prefilled workspace_id from profile: {wid}[/dim]")
    # If something is still missing, try the SDK as a best-effort fallback.
    # Stay quiet on auth failures — the user may just not be logged in yet.
    if cfg.workspace_url and cfg.workspace_id:
        return
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(profile=cfg.profile)
        if not cfg.workspace_url and w.config.host:
            cfg.workspace_url = w.config.host.rstrip("/")
        if not cfg.workspace_id:
            try:
                ma = w.metastores.current()
                if ma and getattr(ma, "workspace_id", None):
                    cfg.workspace_id = str(ma.workspace_id)
            except Exception as e:
                logger.debug("metastores.current() failed: %s", e)
    except Exception as e:
        logger.debug("SDK fallback enrichment failed: %s", e)


# ----------------------------------------------------- Interactive wizard ----

def prompt_field(name: str, label: str, current: str, secret: bool) -> str:
    if secret:
        shown = "****" if current else ""
        msg = f"{label}"
        if shown:
            msg += f" [dim](press Enter to keep existing)[/dim]"
        raw = getpass.getpass(f"{msg}: ")
        return raw if raw else current
    return Prompt.ask(label, default=current or None) or ""


def profile_picker(cfg: Config) -> None:
    profiles = list_cli_profiles()
    logger.info("available CLI profiles: %s", profiles)
    if not profiles:
        say("[yellow]No ~/.databrickscfg profiles found.[/yellow]", level=logging.WARNING)
        return
    console.print("\n[bold]Databricks CLI profiles[/bold]")
    for i, p in enumerate(profiles, 1):
        marker = " [green](last used)[/green]" if p == cfg.profile else ""
        console.print(f"  [{i}] {p}{marker}")
    console.print(f"  [0] (none / skip)")
    default_idx = str(profiles.index(cfg.profile) + 1) if cfg.profile in profiles else "1"
    choice = Prompt.ask("Select profile number", default=default_idx)
    try:
        idx = int(choice)
    except ValueError:
        idx = 0
    cfg.profile = profiles[idx - 1] if 1 <= idx <= len(profiles) else ""
    logger.info("profile selected: '%s' (choice=%s)", cfg.profile, choice)


def interactive_wizard(cfg: Config, only_missing: bool) -> Config:
    console.print()
    console.rule("[bold cyan]Zerobus Feeder — interactive setup[/bold cyan]")

    # Profile selection first so we can prefill workspace_url / workspace_id.
    if not only_missing or not cfg.profile:
        want_profile = Confirm.ask(
            "Use a Databricks CLI profile (needed for --create-sp / --create-table)?",
            default=bool(cfg.profile),
        )
        if want_profile:
            profile_picker(cfg)
            if cfg.profile:
                enrich_from_profile(cfg)

    for name, label, secret, required in PARAMS:
        if name in ("profile", "warehouse_id", "sp_display_name"):
            continue
        current = getattr(cfg, name)
        if only_missing and (current or not required):
            continue
        new_val = prompt_field(name, label, str(current) if current else "", secret)
        if name == "qps":
            try:
                setattr(cfg, name, float(new_val))
            except ValueError:
                console.print(f"[red]Invalid QPS; keeping {cfg.qps}[/red]")
        else:
            setattr(cfg, name, new_val)

    return cfg


# --------------------------------------------------------- Validation ----

def missing_required(cfg: Config) -> list[str]:
    missing = []
    for name, label, _secret, required in PARAMS:
        if not required:
            continue
        val = getattr(cfg, name)
        if val is None or val == "" or (name == "qps" and not val):
            missing.append(label)
    if cfg.cloud and cfg.cloud.lower() not in CLOUDS:
        missing.append(f"cloud must be one of {CLOUDS} (got {cfg.cloud!r})")
    return missing


# ----------------------------------------------------- Data generation ----

class DataGenerator:
    def __init__(self, schema_path: str):
        with open(schema_path) as f:
            self.schema = json.load(f)
        if "columns" not in self.schema or not isinstance(self.schema["columns"], list):
            raise ValueError("Schema JSON must contain a 'columns' list")
        self.columns = self.schema["columns"]
        for col in self.columns:
            if "name" not in col or "type" not in col:
                raise ValueError(f"Column missing 'name' or 'type': {col}")
            if col["type"].lower() not in SUPPORTED_TYPES:
                raise ValueError(
                    f"Unsupported type {col['type']!r} in column {col['name']!r}. "
                    f"Supported: {sorted(SUPPORTED_TYPES)}"
                )

    def generate(self) -> dict:
        return {col["name"]: self._gen(col) for col in self.columns}

    @staticmethod
    def _gen(col: dict) -> Any:
        t = col["type"].lower()
        if col.get("nullable") and random.random() < col.get("null_probability", 0.0):
            return None
        if "choices" in col and t != "boolean":
            return random.choice(col["choices"])

        if t == "string":
            if "length" in col:
                n = int(col["length"])
            else:
                n = random.randint(int(col.get("min_length", 8)), int(col.get("max_length", 16)))
            prefix = col.get("prefix", "")
            alphabet = col.get("alphabet", string.ascii_letters + string.digits)
            return prefix + "".join(random.choices(alphabet, k=n))

        if t in ("int", "long"):
            lo = int(col.get("min", 0))
            hi = int(col.get("max", 1_000_000))
            return random.randint(lo, hi)

        if t in ("float", "double"):
            lo = float(col.get("min", 0.0))
            hi = float(col.get("max", 1.0))
            return round(random.uniform(lo, hi), int(col.get("precision", 6)))

        if t == "boolean":
            p = float(col.get("true_probability", 0.5))
            return random.random() < p

        if t == "timestamp":
            # Zerobus expects Unix microseconds for TIMESTAMP.
            offset = float(col.get("offset_seconds", 0))
            jitter = float(col.get("jitter_seconds", 0))
            sec = time.time() + offset + random.uniform(-jitter, jitter)
            return int(sec * 1_000_000)

        if t == "date":
            # Epoch days.
            offset_days = int(col.get("offset_days", 0))
            range_days = int(col.get("range_days", 0))
            d = date.today() + timedelta(days=offset_days)
            if range_days:
                d += timedelta(days=random.randint(-range_days, range_days))
            return (d - date(1970, 1, 1)).days

        if t == "binary":
            n = int(col.get("length", 16))
            return base64.b64encode(os.urandom(n)).decode("ascii")

        raise ValueError(f"Unsupported type: {t}")


# ----------------------------------------------------------- Stats / UI ----

class Stats:
    def __init__(self, history: int = 120):
        self.sent = 0
        self.errors = 0
        self.started = time.time()
        self.hist = deque(maxlen=history)
        self.window = deque(maxlen=2000)
        self.last_error: str = ""
        self._lock = threading.Lock()

    def record_ok(self, latency_ms: float) -> None:
        with self._lock:
            self.sent += 1
            self.hist.append(latency_ms)
            self.window.append(latency_ms)

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = message

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = max(1e-9, time.time() - self.started)
            window = sorted(self.window)
            n = len(window)

            def pct(p: float) -> float:
                if not n:
                    return 0.0
                return window[min(n - 1, int(n * p))]

            return {
                "sent": self.sent,
                "errors": self.errors,
                "elapsed": elapsed,
                "qps_actual": self.sent / elapsed,
                "last_error": self.last_error,
                "cur": self.hist[-1] if self.hist else 0.0,
                "min": window[0] if n else 0.0,
                "max": window[-1] if n else 0.0,
                "p50": pct(0.50),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "history": list(self.hist),
            }


SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"


def sparkline(values: list[float], width: int = 80) -> str:
    if not values:
        return ""
    if len(values) > width:
        step = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = list(values)
    lo, hi = min(sampled), max(sampled)
    rng = max(1e-9, hi - lo)
    out = []
    for v in sampled:
        idx = int((v - lo) / rng * (len(SPARK_BLOCKS) - 1))
        out.append(SPARK_BLOCKS[max(0, min(len(SPARK_BLOCKS) - 1, idx))])
    return "".join(out)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}"


def render_dashboard(snap: dict, cfg: Config, target_qps: float) -> Panel:
    stats = Table.grid(padding=(0, 2), expand=True)
    stats.add_column(style="bold")
    stats.add_column()
    stats.add_column(style="bold")
    stats.add_column()

    stats.add_row(
        "Sent:", f"{snap['sent']:,}",
        "Errors:", f"[{'red' if snap['errors'] else 'dim'}]{snap['errors']:,}[/]",
    )
    stats.add_row(
        "Elapsed:", format_duration(snap["elapsed"]),
        "QPS (target/actual):", f"{target_qps:.1f} / {snap['qps_actual']:.1f}",
    )
    stats.add_row(
        "Latency now:", f"{snap['cur']:.2f} ms",
        "p50 / p95 / p99:",
        f"{snap['p50']:.2f} / {snap['p95']:.2f} / {snap['p99']:.2f} ms",
    )
    stats.add_row(
        "Latency min / max:", f"{snap['min']:.2f} / {snap['max']:.2f} ms",
        "Target:", f"{cfg.table_name}",
    )
    stats.add_row(
        "Endpoint:", cfg.zerobus_endpoint() if cfg.workspace_id else "(unset)",
        "Workspace URL:", cfg.workspace_url or "(unset)",
    )

    hist = snap["history"]
    spark = sparkline(hist, width=80) if hist else ""
    scale = ""
    if hist:
        scale = f"min {min(hist):.1f} ms  max {max(hist):.1f} ms  (last {len(hist)} samples)"
    spark_block = Group(
        Text(f"Latency  {spark}", style="green"),
        Text(scale, style="dim"),
    )

    body: list[Any] = [stats, Text(""), spark_block]
    if snap["last_error"]:
        body.append(Text(""))
        body.append(Text(f"Last error: {snap['last_error']}", style="red"))

    return Panel(
        Group(*body),
        title="[bold cyan]Zerobus Feeder[/bold cyan]",
        subtitle="[dim]Ctrl+C to stop[/dim]",
        border_style="cyan",
    )


# ---------------------------------------------------------- SP + table ----

def _workspace_client(cfg: Config):
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        say("[red]databricks-sdk is required for --create-sp / --create-table.[/red]",
            level=logging.ERROR)
        say("[red]Install: pip install databricks-sdk[/red]", level=logging.ERROR)
        raise SystemExit(1)
    if not cfg.profile:
        say("[red]A Databricks CLI profile is required for --create-sp / --create-table.[/red]",
            level=logging.ERROR)
        raise SystemExit(1)
    logger.info("constructing WorkspaceClient(profile=%s)", cfg.profile)
    try:
        return WorkspaceClient(profile=cfg.profile)
    except Exception as e:
        return _handle_workspace_auth_failure(cfg, e)


def _handle_workspace_auth_failure(cfg: "Config", original: Exception):
    """Offer to run `databricks auth login --profile <name>` when the SDK
    cannot authenticate, then retry the WorkspaceClient construction.
    """
    import shutil
    import subprocess
    from databricks.sdk import WorkspaceClient

    msg = str(original).lower()
    looks_like_auth = (
        "cannot configure default credentials" in msg
        or "default auth" in msg
        or "databricks-cli" in msg
        or "oauth" in msg
        or "token" in msg
    )
    logger.error("WorkspaceClient construction failed: %s", original)
    if not looks_like_auth:
        say(f"[red]Could not initialise workspace client: {original}[/red]",
            level=logging.ERROR)
        raise SystemExit(1)

    say(
        f"\n[yellow]Profile '{cfg.profile}' has no active Databricks auth session.[/yellow]\n"
        "This profile uses OAuth U2M (auth_type=databricks-cli), which needs a one-time "
        "browser login.",
        level=logging.WARNING,
    )
    if not shutil.which("databricks"):
        say(
            "[red]The 'databricks' CLI is not on PATH.[/red]\n"
            "Install it (https://docs.databricks.com/dev-tools/cli/install.html) and run:\n"
            f"  databricks auth login --profile {cfg.profile}",
            level=logging.ERROR,
        )
        raise SystemExit(1)

    if not Confirm.ask(
        f"Run 'databricks auth login --profile {cfg.profile}' now (opens a browser)?",
        default=True,
    ):
        say(
            f"[yellow]Run this command manually, then re-run the feeder:[/yellow]\n"
            f"  databricks auth login --profile {cfg.profile}",
            level=logging.WARNING,
        )
        raise SystemExit(1)

    logger.info("invoking 'databricks auth login --profile %s'", cfg.profile)
    try:
        subprocess.run(
            ["databricks", "auth", "login", "--profile", cfg.profile],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("databricks auth login exit code %s", e.returncode)
        say(f"[red]Login failed (exit code {e.returncode}).[/red]", level=logging.ERROR)
        raise SystemExit(1)
    except FileNotFoundError:
        say("[red]'databricks' CLI not found after PATH check — aborting.[/red]",
            level=logging.ERROR)
        raise SystemExit(1)

    say("[green]✓[/green] Login completed. Retrying...")
    logger.info("retrying WorkspaceClient after auth login")
    try:
        return WorkspaceClient(profile=cfg.profile)
    except Exception as e:
        logger.exception("WorkspaceClient still failing after login")
        say(f"[red]Still cannot authenticate: {e}[/red]", level=logging.ERROR)
        raise SystemExit(1)


def _create_sp_oauth_secret(w, sp_id) -> str:
    """Create an OAuth client secret for a workspace-level service principal.

    The databricks-sdk WorkspaceClient does not (as of 0.105) expose a
    service_principal_secrets namespace — that only exists on AccountClient
    and requires account-admin. The workspace REST endpoint, however, is
    reachable by workspace admins with Service-principal:Manager, so we call
    it directly through the SDK's generic api_client.

    Endpoint (workspace-addressed):
        POST /api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets
    Response includes the plaintext `secret` (shown once).
    """
    path = f"/api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets"
    logger.info("POST %s", path)
    try:
        resp = w.api_client.do("POST", path)
    except Exception as e:
        logger.warning("OAuth secret REST call failed: %s", e)
        say(f"[yellow]Secret creation call failed: {e}[/yellow]", level=logging.WARNING)
        return ""
    secret = (resp or {}).get("secret", "") if isinstance(resp, dict) else ""
    if not secret:
        logger.warning("secret missing in response: keys=%s",
                       list((resp or {}).keys()) if isinstance(resp, dict) else type(resp))
        return ""
    logger.info("OAuth secret created (length=%d, expire_time=%s)",
                len(secret), resp.get("expire_time"))
    return secret


def create_service_principal(cfg: Config) -> None:
    w = _workspace_client(cfg)
    say(f"[cyan]Creating service principal[/cyan] '{cfg.sp_display_name}'...")
    try:
        sp = w.service_principals.create(display_name=cfg.sp_display_name)
    except Exception as e:
        logger.exception("service_principals.create failed")
        say(f"[red]Failed to create service principal: {e}[/red]", level=logging.ERROR)
        raise SystemExit(1)
    client_id = sp.application_id or ""
    say(f"[green]✓[/green] service principal id={sp.id}  application_id={client_id}")

    say("[cyan]Generating OAuth client secret...[/cyan]")
    client_secret = _create_sp_oauth_secret(w, sp.id)

    if not client_secret:
        say(
            f"[red]Could not create an OAuth secret automatically.[/red]\n"
            f"Create one manually in the Databricks UI:\n"
            f"  {w.config.host.rstrip('/')}/settings/workspace/identity-and-access/service-principals\n"
            f"  → open '{cfg.sp_display_name}' → Secrets → Generate secret\n"
            f"Then re-run with --client-id / --client-secret (or enter them interactively).",
            level=logging.ERROR,
        )
        raise SystemExit(1)

    cfg.client_id = client_id
    cfg.client_secret = client_secret
    say("[green]✓[/green] OAuth secret created and stored in the last-values file.")
    console.print(
        "[bold yellow]Copy the secret now; Databricks will not show it again.[/bold yellow]\n"
        f"  client_id:     {client_id}\n"
        f"  client_secret: {client_secret}"
    )
    logger.info("SP credentials populated: client_id=%s client_secret=%s",
                client_id, _mask(client_secret))


def create_table(cfg: Config) -> None:
    w = _workspace_client(cfg)
    gen = DataGenerator(cfg.schema_file)  # validates schema early
    logger.info("schema loaded from %s with %d columns", cfg.schema_file, len(gen.columns))

    warehouse_id = cfg.warehouse_id or _pick_warehouse(w)
    cfg.warehouse_id = warehouse_id
    logger.info("using warehouse_id=%s", warehouse_id)

    cols = ",\n  ".join(
        f"{c['name']} {DELTA_TYPE_MAP[c['type'].lower()]}"
        for c in gen.columns
    )
    ddl = f"CREATE TABLE IF NOT EXISTS {cfg.table_name} (\n  {cols}\n)"
    say("[cyan]Executing DDL:[/cyan]")
    console.print(Panel(ddl, border_style="dim"))
    logger.info("DDL:\n%s", ddl)
    _execute_sql(w, warehouse_id, ddl)
    say(f"[green]✓[/green] Table {cfg.table_name} ready.")

    if cfg.client_id and Confirm.ask(
        f"Grant USE CATALOG / USE SCHEMA / MODIFY / SELECT to {cfg.client_id} on this table?",
        default=True,
    ):
        try:
            catalog, schema, _ = cfg.table_name.split(".")
        except ValueError:
            say(f"[red]table_name must be catalog.schema.table; got {cfg.table_name!r}[/red]",
                level=logging.ERROR)
            return
        for sql in (
            f"GRANT USE CATALOG ON CATALOG {catalog} TO `{cfg.client_id}`",
            f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{cfg.client_id}`",
            f"GRANT MODIFY, SELECT ON TABLE {cfg.table_name} TO `{cfg.client_id}`",
        ):
            _execute_sql(w, warehouse_id, sql)
        say("[green]✓[/green] Grants applied.")


def _pick_warehouse(w) -> str:
    warehouses = list(w.warehouses.list())
    logger.info("warehouses available: %d", len(warehouses))
    if not warehouses:
        say("[red]No SQL warehouses found in this workspace.[/red]", level=logging.ERROR)
        raise SystemExit(1)
    console.print("\n[bold]SQL warehouses[/bold]")
    for i, wh in enumerate(warehouses, 1):
        state = wh.state.value if wh.state else "?"
        console.print(f"  [{i}] {wh.name}  [dim]({wh.id}, {state})[/dim]")
    choice = Prompt.ask("Select warehouse number", default="1")
    try:
        idx = int(choice) - 1
        chosen = warehouses[idx]
        logger.info("warehouse selected: name=%s id=%s", chosen.name, chosen.id)
        return chosen.id
    except (ValueError, IndexError):
        say("[red]Invalid selection.[/red]", level=logging.ERROR)
        raise SystemExit(1)


def _execute_sql(w, warehouse_id: str, sql: str) -> None:
    from databricks.sdk.service.sql import StatementState
    logger.info("SQL → warehouse=%s: %s", warehouse_id, sql)
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="30s",
    )
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        err = (resp.status.error.message if resp.status and resp.status.error else "unknown")
        logger.error("SQL failed (%s): %s\n%s", state, err, sql)
        say(f"[red]SQL failed ({state}): {err}[/red]\n[dim]{sql}[/dim]", level=logging.ERROR)
        raise SystemExit(1)
    logger.info("SQL succeeded: %s", state)


# --------------------------------------------------------------- Feeder ----

def run_feeder(cfg: Config) -> None:
    try:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk
    except ImportError:
        say("[red]databricks-zerobus-ingest-sdk is required.[/red]", level=logging.ERROR)
        say("[red]Install: pip install databricks-zerobus-ingest-sdk[/red]", level=logging.ERROR)
        sys.exit(1)

    generator = DataGenerator(cfg.schema_file)
    endpoint = cfg.zerobus_endpoint()
    logger.info("feeder starting  endpoint=%s  workspace_url=%s  table=%s  target_qps=%.3f  columns=%d",
                endpoint, cfg.workspace_url, cfg.table_name, cfg.qps, len(generator.columns))

    say(f"[cyan]Connecting[/cyan] endpoint={endpoint}")
    say(f"[cyan]Workspace URL[/cyan] {cfg.workspace_url}")
    say(f"[cyan]Table[/cyan] {cfg.table_name}")
    say(f"[cyan]Authenticating[/cyan] as client_id={cfg.client_id}")

    sdk = ZerobusSdk(endpoint, cfg.workspace_url)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    table_props = TableProperties(cfg.table_name)

    try:
        stream = sdk.create_stream(cfg.client_id, cfg.client_secret, table_props, options)
    except Exception as e:
        logger.exception("create_stream failed")
        say(f"[red]Failed to open Zerobus stream: {e}[/red]", level=logging.ERROR)
        sys.exit(1)
    say("[green]✓[/green] Stream opened. Starting ingestion...")

    stats = Stats()
    stop = threading.Event()
    stop_reason = {"value": "normal exit"}

    def _sigint(signum, frame):
        stop_reason["value"] = f"signal {signum}"
        logger.info("received signal %s — stopping feeder", signum)
        stop.set()
    signal.signal(signal.SIGINT, _sigint)

    target_qps = max(0.001, float(cfg.qps))
    interval = 1.0 / target_qps
    next_send = time.perf_counter()
    last_stats_log = time.time()
    last_logged_error = ""

    try:
        with Live(render_dashboard(stats.snapshot(), cfg, target_qps),
                  console=console, refresh_per_second=5, screen=False) as live:
            while not stop.is_set():
                now = time.perf_counter()
                if now < next_send:
                    time.sleep(min(0.2, next_send - now))
                    continue

                record = generator.generate()
                payload = json.dumps(record)
                t0 = time.perf_counter()
                try:
                    stream.ingest_record(payload)
                    stats.record_ok((time.perf_counter() - t0) * 1000.0)
                except Exception as e:
                    msg = str(e)[:200]
                    stats.record_error(msg)
                    # Log only when the error message changes to avoid flooding.
                    if msg != last_logged_error:
                        logger.warning("ingest error: %s", msg)
                        last_logged_error = msg

                next_send += interval
                # If we fell far behind, don't spiral.
                drift = time.perf_counter() - next_send
                if drift > 5.0:
                    next_send = time.perf_counter()

                # Periodic stats dump to the log file.
                if time.time() - last_stats_log >= 5.0:
                    snap = stats.snapshot()
                    logger.info(
                        "stats sent=%d errors=%d elapsed=%.1fs actual_qps=%.2f "
                        "latency cur=%.2fms p50=%.2f p95=%.2f p99=%.2f min=%.2f max=%.2f",
                        snap["sent"], snap["errors"], snap["elapsed"], snap["qps_actual"],
                        snap["cur"], snap["p50"], snap["p95"], snap["p99"],
                        snap["min"], snap["max"],
                    )
                    last_stats_log = time.time()

                live.update(render_dashboard(stats.snapshot(), cfg, target_qps))
    except Exception as e:
        stop_reason["value"] = f"exception: {e}"
        logger.exception("feeder loop crashed")
        raise
    finally:
        say("\n[cyan]Flushing and closing stream...[/cyan]")
        try:
            stream.flush()
            logger.info("stream flushed")
        except Exception as e:
            logger.warning("flush failed: %s", e)
            say(f"[yellow]flush: {e}[/yellow]", level=logging.WARNING)
        try:
            stream.close()
            logger.info("stream closed")
        except Exception as e:
            logger.warning("close failed: %s", e)
            say(f"[yellow]close: {e}[/yellow]", level=logging.WARNING)
        snap = stats.snapshot()
        summary = (
            f"Done. sent={snap['sent']:,}  errors={snap['errors']:,}  "
            f"elapsed={format_duration(snap['elapsed'])}  actual_qps={snap['qps_actual']:.1f}  "
            f"reason={stop_reason['value']}"
        )
        console.print(f"[bold]{summary}[/bold]")
        logger.info("feeder stopped: %s", summary)


# ------------------------------------------------------------- Argparse ----

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="zerobus_feeder",
        description="Send a configurable synthetic data stream to a Databricks Zerobus endpoint.",
    )
    p.add_argument("--config", "-c", help="Path to YAML config file (overrides all other parameters)")
    p.add_argument("--qps", type=float, help="Transmission rate in queries (records) per second")
    p.add_argument("--schema-file", help="Path to JSON file describing the data structure")
    p.add_argument("--workspace-id", help="Workspace ID (digits) used to build the Zerobus endpoint")
    p.add_argument("--region", help="Region code (e.g. us-west-2, eastus)")
    p.add_argument("--cloud", choices=CLOUDS, help="Cloud provider")
    p.add_argument("--table-name", help="Full table name (catalog.schema.table)")
    p.add_argument("--client-id", help="Service Principal client ID")
    p.add_argument("--client-secret", help="Service Principal client secret")
    p.add_argument("--workspace-url", help="Workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)")
    p.add_argument("--profile", help="Databricks CLI profile (optional, used by --create-sp/--create-table)")
    p.add_argument("--warehouse-id", help="SQL Warehouse ID used for --create-table")
    p.add_argument("--sp-display-name", help="Display name for --create-sp")
    p.add_argument("--create-sp", action="store_true",
                   help="Create a service principal (requires --profile) and store its credentials")
    p.add_argument("--create-table", action="store_true",
                   help="Create the target table from the schema file (requires --profile)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--interactive", action="store_true",
                      help="Force interactive prompts for all parameters")
    mode.add_argument("--non-interactive", action="store_true",
                      help="Never prompt; error out if any required parameter is missing")
    return p


def apply_args(cfg: Config, args: argparse.Namespace) -> None:
    mapping = {
        "qps": "qps", "schema_file": "schema_file", "workspace_id": "workspace_id",
        "region": "region", "cloud": "cloud", "table_name": "table_name",
        "client_id": "client_id", "client_secret": "client_secret",
        "workspace_url": "workspace_url", "profile": "profile",
        "warehouse_id": "warehouse_id", "sp_display_name": "sp_display_name",
    }
    for arg_key, cfg_key in mapping.items():
        val = getattr(args, arg_key, None)
        if val is not None:
            setattr(cfg, cfg_key, val)


def first_run() -> bool:
    return not LAST_VALUES_FILE.exists()


# ---------------------------------------------------------------- main ----

def main() -> None:
    setup_logging()
    args = build_parser().parse_args()
    logger.info("parsed args: %s", vars(args))

    cfg = Config()

    if args.config:
        say(f"[cyan]Loading YAML config:[/cyan] {args.config}")
        for k, v in load_yaml_config(args.config).items():
            setattr(cfg, k, v)
        log_config(cfg, "config after YAML load")
    else:
        last = load_last_values()
        if last:
            logger.info("loaded last-values file with %d keys", len(last))
        for k, v in last.items():
            if hasattr(cfg, k) and v is not None:
                setattr(cfg, k, v)
        apply_args(cfg, args)
        log_config(cfg, "config after last-values + CLI args")

    # Detect first run and offer guided setup (only if not using YAML/non-interactive).
    if first_run() and not args.config and not args.non_interactive:
        logger.info("first-run guided setup triggered (no last-values file)")
        console.print(Panel.fit(
            "[bold]Welcome to Zerobus Feeder[/bold]\n"
            "It looks like this is your first run. I'll walk you through:\n"
            "  1. Selecting a Databricks CLI profile\n"
            "  2. (Optional) creating a service principal  [dim]--create-sp[/dim]\n"
            "  3. (Optional) creating the target table     [dim]--create-table[/dim]\n"
            "  4. Starting the data feed",
            border_style="cyan",
        ))
        if not args.profile and not cfg.profile:
            profile_picker(cfg)
            if cfg.profile:
                enrich_from_profile(cfg)
        if not args.create_sp and Confirm.ask("Create a new service principal now?", default=False):
            logger.info("user opted to create SP inline at first-run prompt")
            if not cfg.profile:
                say(
                    "[red]Cannot create a service principal without a Databricks CLI profile.[/red]",
                    level=logging.ERROR,
                )
            else:
                if not cfg.sp_display_name:
                    cfg.sp_display_name = "zerobus-feeder"
                cfg.sp_display_name = Prompt.ask(
                    "Service principal display name", default=cfg.sp_display_name,
                )
                create_service_principal(cfg)
                save_last_values(cfg)
                log_config(cfg, "config after inline SP creation")
                offer_config_yaml_copy(cfg)
                # Already done — don't run --create-sp again later.
                args.create_sp = False
        if not args.create_table and Confirm.ask("Create the target table now?", default=False):
            args.create_table = True
            logger.info("user opted into --create-table at first-run prompt")

    interactive = args.interactive or (
        not args.config and not args.non_interactive and bool(missing_required(cfg))
    )
    if interactive:
        logger.info("entering interactive wizard (forced=%s)", args.interactive)
        interactive_wizard(cfg, only_missing=not args.interactive)
        log_config(cfg, "config after interactive wizard")

    # Secondary enrichment if profile got set but fields still missing.
    if cfg.profile and (not cfg.workspace_url or not cfg.workspace_id):
        enrich_from_profile(cfg)

    # --create-sp must run before validation so client_id/secret get populated.
    if args.create_sp:
        create_service_principal(cfg)
        save_last_values(cfg)
        log_config(cfg, "config after --create-sp")

    if args.create_table:
        if not cfg.schema_file:
            say("[red]--create-table requires --schema-file.[/red]", level=logging.ERROR)
            sys.exit(2)
        if not cfg.table_name:
            say("[red]--create-table requires --table-name.[/red]", level=logging.ERROR)
            sys.exit(2)
        create_table(cfg)
        save_last_values(cfg)

    missing = missing_required(cfg)
    if missing:
        say("[red]Missing or invalid required parameters:[/red]", level=logging.ERROR)
        for m in missing:
            say(f"  • {m}", level=logging.ERROR)
        console.print(
            "\nProvide them via CLI flags, a YAML config (--config), or run interactively "
            "(omit --non-interactive)."
        )
        sys.exit(2)

    save_last_values(cfg)
    run_feeder(cfg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Aborted by user.[/yellow]")
        logger.info("aborted by user (KeyboardInterrupt)")
        sys.exit(130)
    except EOFError:
        # Ctrl+D at an interactive prompt.
        console.print("\n[yellow]Input closed; aborting.[/yellow]")
        logger.info("aborted (EOF on stdin)")
        sys.exit(130)
    except SystemExit as e:
        logger.info("exit code=%s", e.code)
        raise
    except Exception:
        logger.exception("unhandled exception")
        console.print_exception()
        sys.exit(1)
    else:
        logger.info("session ended cleanly")
