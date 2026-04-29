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
import math
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
    eps: float = 10.0
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
    disable_table_latency: bool = False

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
    ("eps",           "Transmission rate (EPS — events / sec)", False, True),
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


def _ensure_gitignored(path: Path) -> None:
    """Append `path` (as a repo-relative entry) to the project's .gitignore
    if it is not already listed. Only relevant when the file sits inside the
    script directory and a .gitignore exists — otherwise silently no-op.
    """
    gitignore = SCRIPT_DIR / ".gitignore"
    if not gitignore.exists():
        return
    try:
        rel = str(path.resolve().relative_to(SCRIPT_DIR.resolve()))
    except ValueError:
        # File is outside the repo — nothing to do.
        return
    try:
        existing = gitignore.read_text()
    except Exception as e:
        logger.warning("Could not read .gitignore: %s", e)
        return
    # Trivial exact-line match — don't try to interpret glob semantics.
    lines = {ln.strip() for ln in existing.splitlines() if ln.strip() and not ln.startswith("#")}
    if rel in lines:
        logger.info("%s already listed in .gitignore", rel)
        return
    try:
        with gitignore.open("a") as f:
            if not existing.endswith("\n"):
                f.write("\n")
            f.write(
                "\n# Generated config (contains client_secret) — do not commit\n"
                f"{rel}\n"
            )
        say(f"[green]✓[/green] Added {rel} to .gitignore")
        logger.info("appended %s to .gitignore", rel)
    except Exception as e:
        logger.warning("Could not update .gitignore: %s", e)
        say(f"[yellow]Could not update .gitignore ({e}); add {rel} manually.[/yellow]",
            level=logging.WARNING)


def offer_config_yaml_copy(cfg: "Config") -> None:
    """Prompt the user to dump the current config (including just-created SP
    credentials) into a reusable YAML file for `--config`, then ensure the
    file is ignored by git.
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
    except Exception as e:
        logger.exception("write YAML config failed")
        say(f"[red]Could not write {p}: {e}[/red]", level=logging.ERROR)
        return
    _ensure_gitignored(p)


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
        if name == "eps":
            try:
                setattr(cfg, name, float(new_val))
            except ValueError:
                console.print(f"[red]Invalid EPS; keeping {cfg.eps}[/red]")
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
        if val is None or val == "" or (name == "eps" and not val):
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
    """Latency from periodic serial probes; EPS from non-blocking sends."""

    def __init__(self, history_batches: int = 600):
        self.sent = 0              # non-blocking sends only (for EPS)
        self.bytes_sent = 0        # non-blocking payload bytes
        self.errors = 0
        self.started = time.time()
        self.window = deque(maxlen=2000)  # probe latencies for percentiles
        self.last_latency = 0.0
        self.last_error: str = ""
        self.phase: str = "probing"
        self._lock = threading.Lock()

        # Streaming-phase wall time (seconds) — basis for EPS.
        self.stream_elapsed = 0.0
        self._phase_since = time.perf_counter()

        # One (avg_ms, max_ms) entry per probe batch.
        self.hist: deque[tuple[float, float]] = deque(maxlen=history_batches)

        # Last successful table-latency query result (or None).
        self.table_latency: Optional[dict] = None
        # One (avg_sec, p50_sec, p95_sec, p99_sec) entry per successful query.
        self.table_hist: deque[tuple[float, float, float, float]] = deque(maxlen=history_batches)

    def set_phase(self, phase: str) -> None:
        with self._lock:
            now = time.perf_counter()
            if self.phase == "streaming":
                self.stream_elapsed += now - self._phase_since
            self.phase = phase
            self._phase_since = now

    def _stream_elapsed_now(self) -> float:
        """Total streaming-phase wall time including any in-progress window."""
        if self.phase == "streaming":
            return self.stream_elapsed + (time.perf_counter() - self._phase_since)
        return self.stream_elapsed

    def record_probe_batch(self, latencies_ms: list[float]) -> None:
        """Record a batch of serial probe latencies (blocks on server ACK)."""
        if not latencies_ms:
            return
        with self._lock:
            self.window.extend(latencies_ms)
            self.last_latency = latencies_ms[-1]
            avg = sum(latencies_ms) / len(latencies_ms)
            self.hist.append((avg, max(latencies_ms)))

    def record_sent(self, payload_bytes: int = 0) -> None:
        """Count a non-blocking send (used for EPS and bytes)."""
        with self._lock:
            self.sent += 1
            self.bytes_sent += payload_bytes

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = message

    def record_table_latency_summary(self, result: Optional[dict]) -> None:
        """Latest 15-minute query result, displayed in the summary row."""
        with self._lock:
            if result is not None:
                self.table_latency = result

    def record_table_latency_graph(self, result: Optional[dict]) -> None:
        """Append a 30-second window result to the per-batch graph history."""
        with self._lock:
            if result is None:
                return
            vals = [result.get(k) for k in (
                "avg_latency_sec", "p50_latency_sec",
                "p95_latency_sec", "p99_latency_sec",
            )]
            if all(isinstance(v, (int, float)) for v in vals):
                self.table_hist.append(tuple(float(v) for v in vals))  # type: ignore[arg-type]

    def snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            elapsed = max(1e-9, now - self.started)
            stream_elapsed = self._stream_elapsed_now()
            window = sorted(self.window)
            n = len(window)

            def pct(p: float) -> float:
                if not n:
                    return 0.0
                return window[min(n - 1, int(n * p))]

            if self.hist:
                last_avg, last_max = self.hist[-1]
            else:
                last_avg = last_max = 0.0

            return {
                "sent": self.sent,
                "bytes_sent": self.bytes_sent,
                "errors": self.errors,
                "elapsed": elapsed,
                "stream_elapsed": stream_elapsed,
                "eps_actual": self.sent / stream_elapsed if stream_elapsed > 0 else 0.0,
                "last_error": self.last_error,
                "cur": self.last_latency,
                "last_avg": last_avg,
                "last_max": last_max,
                "min": window[0] if n else 0.0,
                "max": window[-1] if n else 0.0,
                "p50": pct(0.50),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "history": list(self.hist),  # one entry per probe batch
                "phase": self.phase,
                "table_latency": dict(self.table_latency) if self.table_latency else None,
                "table_history": list(self.table_hist),  # one entry per table-latency query
            }


_BAR_BLOCKS = (" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█")
_LEVELS_PER_ROW = len(_BAR_BLOCKS) - 1  # 8

AVG_STYLE = "bright_green"
MAX_STYLE = "bright_yellow"

# Table-latency band colours, ordered narrowest → widest band.
# Each cell of the bar is coloured by the smallest percentile whose level
# still covers it: avg ≤ p50 ≤ p95 ≤ p99 (with avg sometimes above p50).
P50_STYLE = "bright_cyan"
P95_STYLE = "bright_yellow"
P99_STYLE = "bright_red"


def latency_graph(
    buckets: list[tuple[float, float]],
    width: int,
    height: int = 10,
    use_log: bool = True,
) -> tuple[list[Text], float, float]:
    """Render a per-second latency graph.

    Each column is one second. The bar is drawn up to the second's MAX
    latency in MAX_STYLE; the portion from 0 to that second's AVG is
    over-painted in AVG_STYLE so both values are visible simultaneously.

    Returns (styled Text rows, y_min_ms, y_max_ms).
    """
    width = max(10, int(width))
    if not buckets:
        return [], 0.0, 0.0

    # Keep the most recent `width` buckets, right-aligned.
    if len(buckets) > width:
        data = list(buckets)[-width:]
    else:
        data = list(buckets)
    leading_pad = width - len(data)

    all_vals = [v for pair in data for v in pair if v > 0]
    if not all_vals:
        # Only zero-valued buckets so far; render empty rows.
        return [Text(" " * width, style="dim") for _ in range(height)], 0.0, 0.0

    def transform(v: float) -> float:
        return math.log10(max(v, 0.01)) if use_log else v

    t_vals = [transform(v) for v in all_vals]
    t_lo = min(t_vals)
    t_hi = max(t_vals)
    if t_hi - t_lo < 1e-9:
        pad = 0.1 if use_log else max(1e-6, abs(t_hi) * 0.1)
        t_lo -= pad
        t_hi += pad
    rng = t_hi - t_lo
    total_levels = height * _LEVELS_PER_ROW

    def level_of(v: float) -> int:
        if v <= 0:
            return 0
        return max(0, min(total_levels,
                          int(round((transform(v) - t_lo) / rng * total_levels))))

    avg_levels = [level_of(a) for (a, _) in data]
    max_levels = [level_of(m) for (_, m) in data]

    rows: list[Text] = []
    for r in range(height, 0, -1):
        lower = (r - 1) * _LEVELS_PER_ROW
        upper = r * _LEVELS_PER_ROW
        mid = lower + _LEVELS_PER_ROW / 2
        row = Text()
        if leading_pad > 0:
            row.append(" " * leading_pad)
        for al, ml in zip(avg_levels, max_levels):
            # Bar height at this column = ml (max-latency level).
            if ml >= upper:
                ch = _BAR_BLOCKS[_LEVELS_PER_ROW]
            elif ml <= lower:
                ch = _BAR_BLOCKS[0]
            else:
                ch = _BAR_BLOCKS[ml - lower]
            # Colour: green if the middle of this row sits within the
            # avg portion of the bar, yellow if strictly above it.
            style = AVG_STYLE if mid <= al else MAX_STYLE
            row.append(ch, style=style)
        rows.append(row)

    y_min = (10 ** t_lo) if use_log else t_lo
    y_max = (10 ** t_hi) if use_log else t_hi
    return rows, y_min, y_max


def multi_band_graph(
    buckets: list[tuple[float, float, float, float]],
    width: int,
    height: int = 10,
) -> tuple[list[Text], float, float]:
    """Render a per-batch graph with four overlaid percentile bands.

    Each tuple is (avg, p50, p95, p99). The bar height per column is the
    largest of the four; each cell is coloured by the *smallest* band
    whose level still contains it (so a cell is "avg-green" if it sits
    below avg, otherwise "p50-cyan" if below p50, etc.).

    Always linear, y-axis pinned to 0 so absolute magnitudes are obvious.
    """
    width = max(10, int(width))
    if not buckets:
        return [], 0.0, 0.0

    if len(buckets) > width:
        data = list(buckets)[-width:]
    else:
        data = list(buckets)
    leading_pad = width - len(data)

    tip_vals = [max(tup) for tup in data]
    t_hi = max(tip_vals) if tip_vals else 0.0
    if t_hi <= 0:
        return [Text(" " * width, style="dim") for _ in range(height)], 0.0, 0.0
    total_levels = height * _LEVELS_PER_ROW

    def level_of(v: float) -> int:
        if v <= 0:
            return 0
        return max(0, min(total_levels, int(round(v / t_hi * total_levels))))

    # Per-column levels for each band, plus the bar tip (max of the four).
    cols = [
        tuple(level_of(v) for v in tup) + (level_of(max(tup)),)
        for tup in data
    ]
    band_styles = (AVG_STYLE, P50_STYLE, P95_STYLE, P99_STYLE)

    rows: list[Text] = []
    for r in range(height, 0, -1):
        lower = (r - 1) * _LEVELS_PER_ROW
        upper = r * _LEVELS_PER_ROW
        mid = lower + _LEVELS_PER_ROW / 2
        row = Text()
        if leading_pad > 0:
            row.append(" " * leading_pad)
        for avg_l, p50_l, p95_l, p99_l, tip in cols:
            if tip >= upper:
                ch = _BAR_BLOCKS[_LEVELS_PER_ROW]
            elif tip <= lower:
                ch = _BAR_BLOCKS[0]
            else:
                ch = _BAR_BLOCKS[tip - lower]
            # Pick the smallest band whose level still covers `mid`.
            covers = [(lvl, st) for lvl, st in zip(
                (avg_l, p50_l, p95_l, p99_l), band_styles,
            ) if lvl >= mid]
            style = min(covers, key=lambda x: x[0])[1] if covers else "dim"
            row.append(ch, style=style)
        rows.append(row)

    return rows, 0.0, t_hi


def format_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}"


def render_dashboard(snap: dict, cfg: Config, target_eps: float) -> Panel:
    stats = Table.grid(padding=(0, 2), expand=True)
    stats.add_column(style="bold")
    stats.add_column()
    stats.add_column(style="bold")
    stats.add_column()

    VAL_STYLE = "bold bright_cyan"
    def hi(s: Any) -> str:
        return f"[{VAL_STYLE}]{s}[/]"

    phases = {
        "probing":   ("● measuring latency (10 serial probes)", "yellow"),
        "querying":  ("● querying ingest latency (SQL)",        "cyan"),
        "streaming": ("● pushing max messages (non-blocking)",  "green"),
    }
    raw_label, phase_color = phases.get(
        snap.get("phase", ""),
        (f"● {snap.get('phase', 'starting')}", "dim"),
    )
    # Pad to the longest known label so column widths don't jitter when the
    # phase changes (probing → querying → streaming).
    phase_w = max(len(v[0]) for v in phases.values())
    phase_label = raw_label.ljust(phase_w)
    stats.add_row(
        "Status:", f"[{phase_color}]{phase_label}[/{phase_color}]",
        "", "",
    )
    err_color = "red" if snap['errors'] else "dim"
    sent_n = f"{snap['sent']:,}"
    bytes_n = format_bytes(snap['bytes_sent'])
    eps_t = f"{target_eps:.1f}"
    eps_a = f"{snap['eps_actual']:.1f}"
    cur = f"{snap['cur']:.2f}"
    p50 = f"{snap['p50']:.2f}"
    p95 = f"{snap['p95']:.2f}"
    p99 = f"{snap['p99']:.2f}"
    lmin = f"{snap['min']:.2f}"
    lmax = f"{snap['max']:.2f}"
    lavg = f"{snap['last_avg']:.2f}"
    lmaxp = f"{snap['last_max']:.2f}"
    stats.add_row(
        "Sent:", f"{hi(sent_n)}  ({hi(bytes_n)})",
        "Errors:", f"[{err_color}]{snap['errors']:,}[/]",
    )
    stats.add_row(
        "Elapsed:", hi(format_duration(snap["elapsed"])),
        "EPS (target/actual):", f"{hi(eps_t)} / {hi(eps_a)}",
    )
    stats.add_row(
        "Latency now:", f"{hi(cur)} ms",
        "p50 / p95 / p99:", f"{hi(p50)} / {hi(p95)} / {hi(p99)} ms",
    )
    stats.add_row(
        "Latency min / max:", f"{hi(lmin)} / {hi(lmax)} ms",
        "Last probe avg / max:", f"{hi(lavg)} / {hi(lmaxp)} ms",
    )
    stats.add_row(
        "Target:", f"{cfg.table_name}",
        "Endpoint:", cfg.zerobus_endpoint() if cfg.workspace_id else "(unset)",
    )
    tl = snap.get("table_latency")
    if tl:
        avg = tl.get("avg_latency_sec")
        avg_s = f"{avg:.2f}" if isinstance(avg, (int, float)) else "—"
        def n(key: str) -> str:
            v = tl.get(key)
            if v is None:
                return "—"
            if isinstance(v, float):
                return hi(f"{v:.2f}")
            return hi(v)
        stats.add_row(
            "Table latency (15m):",
            f"avg {hi(avg_s)}s  p50 {n('p50_latency_sec')}s  "
            f"p95 {n('p95_latency_sec')}s  p99 {n('p99_latency_sec')}s",
            "Rows / range:",
            f"{n('total_rows')}  "
            f"({n('min_latency_sec')}s … {n('max_latency_sec')}s)",
        )

    hist = snap["history"]
    table_hist = snap.get("table_history", [])
    label_w = 11   # "  1234.5 ms" / "    1.50 s"
    side_pad = 1   # one blank column each side of the graph
    # Panel chrome = 2 border chars + 2 default horizontal padding chars.
    panel_chrome = 4
    middle_gap = "   "  # spacing between the two halves
    # Per-half row layout: [label][space*(side_pad+1)][graph][space*side_pad]
    half_chrome = label_w + (side_pad + 1) + side_pad
    term_w = console.size.width or 120
    graph_width = max(20, (term_w - panel_chrome - 2 * half_chrome - len(middle_gap)) // 2)
    graph_height = 12

    probe_rows, p_ymin, p_ymax = latency_graph(
        hist, width=graph_width, height=graph_height, use_log=True,
    )
    table_rows, t_ymin, t_ymax = multi_band_graph(
        table_hist, width=graph_width, height=graph_height,
    )
    if not probe_rows:
        probe_rows = [Text(" " * graph_width, style="dim") for _ in range(graph_height)]
    if not table_rows:
        table_rows = [Text(" " * graph_width, style="dim") for _ in range(graph_height)]

    def fmt_y(v: float, unit: str) -> str:
        # Right-align into label_w including the unit suffix.
        num_w = max(1, label_w - 1 - len(unit))
        if v >= 1000:
            return f"{v:>{num_w}.0f} {unit}"
        if v >= 10:
            return f"{v:>{num_w}.1f} {unit}"
        return f"{v:>{num_w}.2f} {unit}"

    def build_labels(rows, y_min, y_max, unit, *, log_scale):
        if not rows or (y_min == 0.0 and y_max == 0.0):
            return [" " * label_w] * len(rows)
        mid_idx = len(rows) // 2
        if log_scale:
            mid_val = 10 ** ((math.log10(max(y_min, 1e-3)) + math.log10(max(y_max, 1e-3))) / 2)
        else:
            mid_val = (y_min + y_max) / 2
        out = []
        for i in range(len(rows)):
            if i == 0:
                out.append(fmt_y(y_max, unit))
            elif i == len(rows) - 1:
                out.append(fmt_y(y_min, unit))
            elif i == mid_idx:
                out.append(fmt_y(mid_val, unit))
            else:
                out.append(" " * label_w)
        return out

    left_gutter = " " * (side_pad + 1)
    right_gutter = " " * side_pad
    half_w = label_w + (side_pad + 1) + graph_width + side_pad

    def center_title(s: str, w: int) -> str:
        s = s if len(s) <= w else s[: max(0, w)]
        pad = (w - len(s)) // 2
        return " " * pad + s + " " * (w - len(s) - pad)

    titles = Text(
        center_title("Probe latency (ms, per-batch avg / max)", half_w)
        + middle_gap
        + center_title("Table latency (s, event_time → file_modification_time)", half_w),
        style="bold dim",
    )

    p_labels = build_labels(probe_rows, p_ymin, p_ymax, "ms", log_scale=True)
    t_labels = build_labels(table_rows, t_ymin, t_ymax, "s", log_scale=False)

    labeled_rows: list[Text] = []
    for i in range(graph_height):
        labeled_rows.append(
            Text(p_labels[i] + left_gutter, style="dim")
            + probe_rows[i]
            + Text(right_gutter + middle_gap)
            + Text(t_labels[i] + left_gutter, style="dim")
            + table_rows[i]
            + Text(right_gutter)
        )

    half_rule = "─" * graph_width
    gutter_prefix = " " * label_w + left_gutter
    rule = Text(
        gutter_prefix + half_rule + right_gutter
        + middle_gap
        + gutter_prefix + half_rule + right_gutter,
        style="dim",
    )
    legend = Text.assemble(
        (gutter_prefix, "dim"),
        ("probe ", "dim"),
        ("█ avg  ", AVG_STYLE),
        ("█ max     ", MAX_STYLE),
        ("table ", "dim"),
        ("█ avg  ", AVG_STYLE),
        ("█ p50  ", P50_STYLE),
        ("█ p95  ", P95_STYLE),
        ("█ p99  ", P99_STYLE),
        ("   ", "dim"),
        ("probe: log scale  table: linear from 0   newest →   ", "dim"),
        (f"probes: {len(hist)}  table queries: {len(table_hist)}", "dim"),
    )
    graph_block = Group(titles, *labeled_rows, rule, legend)

    body: list[Any] = [stats, Text(""), graph_block]
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

    _ensure_catalog_and_schema(w, warehouse_id, cfg.table_name)

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
        _apply_zerobus_grants(w, warehouse_id, cfg.table_name, cfg.client_id)


def _split_table_name(table_name: str) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"table_name must be catalog.schema.table; got {table_name!r}")
    return parts[0], parts[1], parts[2]


def _catalog_exists(w, name: str) -> Optional[bool]:
    """Return True if the catalog exists, False if it doesn't, None if the
    existence check itself failed (permission denied etc.)."""
    try:
        from databricks.sdk.errors import NotFound
    except Exception:
        NotFound = ()  # type: ignore
    try:
        w.catalogs.get(name)
        return True
    except NotFound:  # type: ignore[misc]
        return False
    except Exception as e:
        logger.debug("catalogs.get(%s) failed: %s", name, e)
        return None


def _schema_exists(w, catalog: str, schema: str) -> Optional[bool]:
    try:
        from databricks.sdk.errors import NotFound
    except Exception:
        NotFound = ()  # type: ignore
    try:
        w.schemas.get(f"{catalog}.{schema}")
        return True
    except NotFound:  # type: ignore[misc]
        return False
    except Exception as e:
        logger.debug("schemas.get(%s.%s) failed: %s", catalog, schema, e)
        return None


def _ensure_catalog_and_schema(w, warehouse_id: str, table_name: str) -> None:
    """Ensure the catalog and schema from `table_name` exist; offer to create
    them when they don't. Runs before the CREATE TABLE DDL so the DDL itself
    doesn't fail on a missing parent object."""
    catalog, schema, _ = _split_table_name(table_name)

    cat_state = _catalog_exists(w, catalog)
    if cat_state is True:
        logger.info("catalog %s exists", catalog)
    elif cat_state is False:
        say(f"[yellow]Catalog '{catalog}' does not exist.[/yellow]", level=logging.WARNING)
        if not Confirm.ask(f"Create catalog '{catalog}' now?", default=True):
            say(f"[red]Cannot continue without catalog {catalog}.[/red]", level=logging.ERROR)
            raise SystemExit(1)
        _execute_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog}")
        say(f"[green]✓[/green] Catalog {catalog} created.")
    else:
        logger.info("catalog existence check for %s inconclusive; assuming present", catalog)

    sch_state = _schema_exists(w, catalog, schema)
    if sch_state is True:
        logger.info("schema %s.%s exists", catalog, schema)
    elif sch_state is False:
        say(f"[yellow]Schema '{catalog}.{schema}' does not exist.[/yellow]", level=logging.WARNING)
        if not Confirm.ask(f"Create schema '{catalog}.{schema}' now?", default=True):
            say(f"[red]Cannot continue without schema {catalog}.{schema}.[/red]",
                level=logging.ERROR)
            raise SystemExit(1)
        _execute_sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        say(f"[green]✓[/green] Schema {catalog}.{schema} created.")
    else:
        logger.info("schema existence check for %s.%s inconclusive; assuming present",
                    catalog, schema)


def _apply_zerobus_grants(w, warehouse_id: str, table_name: str, principal: str) -> None:
    """Grant the catalog/schema/table permissions Zerobus requires for a
    service principal. Zerobus rejects schema-level inheritance, so the
    table-level GRANT MODIFY, SELECT must be explicit.
    """
    catalog, schema, _ = _split_table_name(table_name)
    sqls = (
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{principal}`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`",
        f"GRANT MODIFY, SELECT ON TABLE {table_name} TO `{principal}`",
    )
    for sql in sqls:
        _execute_sql(w, warehouse_id, sql)
    say("[green]✓[/green] Grants applied.")


def _fixup_missing_grants(cfg: "Config") -> bool:
    """Interactively apply the grants Zerobus needs. Returns True if the
    grants were applied and the caller should retry the stream.
    """
    if not cfg.table_name or not cfg.client_id:
        return False
    try:
        catalog, schema, _ = _split_table_name(cfg.table_name)
    except ValueError as e:
        say(f"[red]{e}[/red]", level=logging.ERROR)
        return False

    say(
        "\n[yellow]Zerobus rejected the service principal's authorizations.[/yellow]\n"
        "The SP is missing explicit table-level grants. Required SQL:"
    )
    grants_sql = (
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{cfg.client_id}`;\n"
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{cfg.client_id}`;\n"
        f"GRANT MODIFY, SELECT ON TABLE {cfg.table_name} TO `{cfg.client_id}`;"
    )
    console.print(Panel(grants_sql, border_style="dim"))

    if not cfg.profile:
        say(
            "[yellow]Set --profile (a Databricks CLI profile with admin rights) so I can apply these for you,\n"
            "or run the statements above in a SQL editor / notebook, then rerun.[/yellow]",
            level=logging.WARNING,
        )
        return False

    if not Confirm.ask("Apply these grants now using your CLI profile?", default=True):
        return False

    try:
        w = _workspace_client(cfg)
        warehouse_id = cfg.warehouse_id or _pick_warehouse(w)
        cfg.warehouse_id = warehouse_id
        _apply_zerobus_grants(w, warehouse_id, cfg.table_name, cfg.client_id)
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("_fixup_missing_grants failed")
        say(f"[red]Could not apply grants: {e}[/red]", level=logging.ERROR)
        return False
    return True


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


# Latency in seconds with microsecond precision, rounded to 2 decimals
# (10ms granularity). The window length is plugged in at format time.
_TABLE_LATENCY_SQL = """
SELECT
  count(*)                                                                                                                      AS total_rows,
  min(event_time)                                                                                                               AS earliest_event,
  max(event_time)                                                                                                               AS latest_event,
  round(avg              ((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0     ), 2)       AS avg_latency_sec,
  round(min              ((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0     ), 2)       AS min_latency_sec,
  round(max              ((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0     ), 2)       AS max_latency_sec,
  round(percentile_approx((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0, 0.5 ), 2)      AS p50_latency_sec,
  round(percentile_approx((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0, 0.95), 2)      AS p95_latency_sec,
  round(percentile_approx((unix_micros(_metadata.file_modification_time) - unix_micros(event_time)) / 1000000.0, 0.99), 2)      AS p99_latency_sec
FROM {table}
WHERE event_time >= current_timestamp() - INTERVAL {window_seconds} SECONDS
""".strip()


def _query_table_latency(
    w, warehouse_id: str, table_name: str, window_seconds: int,
) -> Optional[dict]:
    """Run the ingest-latency query against the target table.

    `window_seconds` is the WHERE-clause lookback (e.g. 30 for the graph,
    900 for the summary).

    Returns a dict of named values, or None if the query failed/timed out.
    Bounded by a 15s warehouse-side timeout so we never block streaming.
    """
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout, StatementState,
    )
    sql = _TABLE_LATENCY_SQL.format(
        table=table_name, window_seconds=int(window_seconds),
    )
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="15s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
        )
        state = resp.status.state if resp.status else None
        if state != StatementState.SUCCEEDED:
            err = (resp.status.error.message
                   if resp.status and resp.status.error else state)
            logger.warning("table-latency query did not succeed: %s", err)
            return None
        cols = [c.name for c in resp.manifest.schema.columns]
        row = resp.result.data_array[0] if resp.result and resp.result.data_array else None
        if not row:
            return None
        out: dict[str, Any] = dict(zip(cols, row))
        # Numeric fields come back as strings via the JSON-array result format.
        if out.get("total_rows") is not None:
            try:
                out["total_rows"] = int(float(out["total_rows"]))
            except (TypeError, ValueError):
                pass
        for k in ("avg_latency_sec", "min_latency_sec", "max_latency_sec",
                  "p50_latency_sec", "p95_latency_sec", "p99_latency_sec"):
            if out.get(k) is not None:
                try:
                    out[k] = float(out[k])
                except (TypeError, ValueError):
                    pass
        return out
    except Exception as e:
        logger.warning("table-latency query failed: %s", e)
        return None


# --------------------------------------------------------------- Feeder ----

def run_feeder(cfg: Config) -> None:
    # Silence the Rust SDK's tracing_subscriber so it doesn't scribble on
    # stderr inside the Rich Live dashboard. Must be set before SDK import.
    os.environ.setdefault("RUST_LOG", "error")
    try:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk
    except ImportError:
        say("[red]databricks-zerobus-ingest-sdk is required.[/red]", level=logging.ERROR)
        say("[red]Install: pip install databricks-zerobus-ingest-sdk[/red]", level=logging.ERROR)
        sys.exit(1)

    generator = DataGenerator(cfg.schema_file)
    endpoint = cfg.zerobus_endpoint()
    logger.info("feeder starting  endpoint=%s  workspace_url=%s  table=%s  target_eps=%.3f  columns=%d",
                endpoint, cfg.workspace_url, cfg.table_name, cfg.eps, len(generator.columns))

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
        msg = str(e)
        auth_err = (
            "invalid_authorization_details" in msg
            or "User is not authorized" in msg
            or "401" in msg
        )
        if auth_err and _fixup_missing_grants(cfg):
            say("[cyan]Retrying stream open after applying grants...[/cyan]")
            time.sleep(2)  # brief pause for grant propagation
            try:
                stream = sdk.create_stream(cfg.client_id, cfg.client_secret, table_props, options)
            except Exception as e2:
                logger.exception("create_stream failed after grants")
                say(f"[red]Still failed after applying grants: {e2}[/red]", level=logging.ERROR)
                sys.exit(1)
        else:
            logger.exception("create_stream failed")
            say(f"[red]Failed to open Zerobus stream: {e}[/red]", level=logging.ERROR)
            sys.exit(1)
    say("[green]✓[/green] Stream opened. Starting ingestion...")

    # Optional: SQL Statement client for the periodic table-latency query.
    # Runs after each probe batch, before resuming non-blocking sends.
    tl_client = None
    if cfg.disable_table_latency:
        say("[dim]Table-latency query disabled (--no-table-latency).[/dim]")
    else:
        if cfg.profile:
            try:
                from databricks.sdk import WorkspaceClient
                tl_client = WorkspaceClient(profile=cfg.profile)
            except Exception as e:
                logger.warning("table-latency: WorkspaceClient unavailable (%s)", e)
        if tl_client is not None and not cfg.warehouse_id and sys.stdin.isatty():
            say("[cyan]No SQL warehouse stored — pick one for the ingest-latency query "
                "(or Ctrl+C to skip).[/cyan]")
            try:
                cfg.warehouse_id = _pick_warehouse(tl_client)
                save_last_values(cfg)
            except (SystemExit, KeyboardInterrupt):
                cfg.warehouse_id = ""
        if tl_client is None or not cfg.warehouse_id:
            say("[dim]Table-latency query disabled (no warehouse configured).[/dim]")
            tl_client = None

    stats = Stats()
    # Cycle: non-blocking push → probe → table-latency → repeat. Start in
    # the streaming phase so the dashboard reflects what's actually happening.
    stats.set_phase("streaming")
    stop = threading.Event()
    stop_reason = {"value": "normal exit"}

    def _sigint(signum, frame):
        stop_reason["value"] = f"signal {signum}"
        logger.info("received signal %s — stopping feeder", signum)
        stop.set()
    signal.signal(signal.SIGINT, _sigint)

    target_eps = max(0.001, float(cfg.eps))
    interval = 1.0 / target_eps
    next_send = time.perf_counter()
    last_stats_log = time.time()
    last_logged_error = ""

    probe_count = 10
    probe_every_secs = 10.0
    # Stream first; defer the first probe by one full interval so the
    # table-latency query at the end of the cycle has data to query.
    next_probe_at = time.perf_counter() + probe_every_secs
    last_ui_update = 0.0
    ui_refresh_secs = 0.2

    try:
        with Live(render_dashboard(stats.snapshot(), cfg, target_eps),
                  console=console, refresh_per_second=5, screen=False) as live:
            while not stop.is_set():
                now = time.perf_counter()

                # Keep the dashboard ticking even when idle between sends.
                if now - last_ui_update >= ui_refresh_secs:
                    live.update(render_dashboard(stats.snapshot(), cfg, target_eps))
                    last_ui_update = now

                # --- Probe phase: 10 serial sends, block on ACK, record latency.
                if now >= next_probe_at:
                    stats.set_phase("probing")
                    live.update(render_dashboard(stats.snapshot(), cfg, target_eps))
                    last_ui_update = time.perf_counter()
                    latencies: list[float] = []
                    for _ in range(probe_count):
                        if stop.is_set():
                            break
                        record = generator.generate()
                        payload = json.dumps(record)
                        t0 = time.perf_counter()
                        try:
                            offset = stream.ingest_record_offset(payload)
                            stream.wait_for_offset(offset)
                            latencies.append((time.perf_counter() - t0) * 1000.0)
                        except Exception as e:
                            msg = str(e)[:200]
                            stats.record_error(msg)
                            if msg != last_logged_error:
                                logger.warning("probe ingest error: %s", msg)
                                last_logged_error = msg
                    stats.record_probe_batch(latencies)

                    # Run the ingest-latency queries before switching back to
                    # non-blocking sends, so timing is taken at a quiet moment.
                    # Two separate windows: 30s for the high-resolution graph,
                    # 15m for the stable summary row in the dashboard header.
                    if tl_client is not None:
                        stats.set_phase("querying")
                        live.update(render_dashboard(stats.snapshot(), cfg, target_eps))
                        last_ui_update = time.perf_counter()
                        for label, window_s, recorder in (
                            ("30s",  30,       stats.record_table_latency_graph),
                            ("15m",  15 * 60,  stats.record_table_latency_summary),
                        ):
                            res = _query_table_latency(
                                tl_client, cfg.warehouse_id, cfg.table_name,
                                window_seconds=window_s,
                            )
                            if res:
                                recorder(res)
                                logger.info(
                                    "table-latency[%s] rows=%s avg=%ss min=%ss max=%ss "
                                    "p50=%ss p95=%ss p99=%ss",
                                    label,
                                    res.get("total_rows"),
                                    res.get("avg_latency_sec"),
                                    res.get("min_latency_sec"),
                                    res.get("max_latency_sec"),
                                    res.get("p50_latency_sec"),
                                    res.get("p95_latency_sec"),
                                    res.get("p99_latency_sec"),
                                )

                    next_probe_at = time.perf_counter() + probe_every_secs
                    next_send = time.perf_counter()
                    stats.set_phase("streaming")
                    continue

                # --- Stream phase: non-blocking sends at target EPS.
                if now < next_send:
                    wake = min(next_send, next_probe_at)
                    time.sleep(min(0.2, max(0.0, wake - now)))
                    continue

                record = generator.generate()
                payload = json.dumps(record)
                try:
                    stream.ingest_record_nowait(payload)
                    stats.record_sent(len(payload.encode("utf-8")))
                except Exception as e:
                    msg = str(e)[:200]
                    stats.record_error(msg)
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
                        "stats sent=%d bytes=%d errors=%d elapsed=%.1fs stream=%.1fs "
                        "actual_eps=%.2f latency cur=%.2fms p50=%.2f p95=%.2f p99=%.2f "
                        "min=%.2f max=%.2f",
                        snap["sent"], snap["bytes_sent"], snap["errors"],
                        snap["elapsed"], snap["stream_elapsed"], snap["eps_actual"],
                        snap["cur"], snap["p50"], snap["p95"], snap["p99"],
                        snap["min"], snap["max"],
                    )
                    last_stats_log = time.time()
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
            f"Done. sent={snap['sent']:,} ({format_bytes(snap['bytes_sent'])})  "
            f"errors={snap['errors']:,}  elapsed={format_duration(snap['elapsed'])}  "
            f"stream={format_duration(snap['stream_elapsed'])}  "
            f"actual_eps={snap['eps_actual']:.1f}  reason={stop_reason['value']}"
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
    p.add_argument("--eps", type=float, help="Transmission rate in events (records) per second")
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
    p.add_argument("--no-table-latency", action="store_true",
                   help="Disable the periodic ingest-latency SQL query "
                        "(skips the SQL warehouse round-trip after each probe)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--interactive", action="store_true",
                      help="Force interactive prompts for all parameters")
    mode.add_argument("--non-interactive", action="store_true",
                      help="Never prompt; error out if any required parameter is missing")
    return p


def apply_args(cfg: Config, args: argparse.Namespace) -> None:
    mapping = {
        "eps": "eps", "schema_file": "schema_file", "workspace_id": "workspace_id",
        "region": "region", "cloud": "cloud", "table_name": "table_name",
        "client_id": "client_id", "client_secret": "client_secret",
        "workspace_url": "workspace_url", "profile": "profile",
        "warehouse_id": "warehouse_id", "sp_display_name": "sp_display_name",
    }
    for arg_key, cfg_key in mapping.items():
        val = getattr(args, arg_key, None)
        if val is not None:
            setattr(cfg, cfg_key, val)
    if getattr(args, "no_table_latency", False):
        cfg.disable_table_latency = True


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
            logger.info("user opted to create table inline at first-run prompt")
            if not cfg.profile:
                say(
                    "[red]Cannot create a table without a Databricks CLI profile.[/red]",
                    level=logging.ERROR,
                )
            else:
                if not cfg.schema_file:
                    default_schema = str(SCRIPT_DIR / "sample_schema.json")
                    cfg.schema_file = Prompt.ask(
                        "Path to data structure JSON", default=default_schema,
                    )
                if not cfg.table_name:
                    cfg.table_name = Prompt.ask(
                        "Full table name (catalog.schema.table)",
                        default=cfg.table_name or None,
                    ) or ""
                if not cfg.schema_file or not cfg.table_name:
                    say(
                        "[red]Both schema file and table name are required — skipping table creation.[/red]",
                        level=logging.ERROR,
                    )
                else:
                    create_table(cfg)
                    save_last_values(cfg)
                    log_config(cfg, "config after inline table creation")
                    # Already done — don't run --create-table again later.
                    args.create_table = False

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
