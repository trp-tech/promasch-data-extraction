"""
Orchestrate Indent Completed extraction: collect → replay → parse → output.json

Usage examples:
  # Full pipeline (Playwright collect + HTTP replay + parse + aggregate):
  uv run python main.py --all --user USER --password PASS

  # Separate steps:
  uv run python main.py --collect --user USER --password PASS --headful
  uv run python main.py --replay  --data-dir data/1234567890
  uv run python main.py --parse   --data-dir data/1234567890

  # Build detail payloads after collecting list responses:
  uv run python main.py --build-detail-payloads --data-dir data/1234567890 \\
      --permutation2 F951AE63290D91FB2B4DEE07BD1C4A5B

  # Parse list dumps only (produces summary JSON, no detail data):
  uv run python main.py --parse-list --data-dir data/1234567890
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from collector import (
    build_detail_payloads_from_catalog,
    ensure_credentials,
    run_collection,
)
from gwt_parser import (
    aggregate_indent_parts,
    parse_detail_dump_file,
    parse_list_dump_file,
)
from replay import replay_catalog

ROOT = Path(__file__).resolve().parent

_DETAIL_METHOD = "GetIndentPartsForProjectIndent"
_LIST_METHOD = "getIndentListCompleted"


# ── Helpers ───────────────────────────────────────────────────────────────────

def log_error(data_dir: Path, message: str) -> None:
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    with (log_dir / "errors.log").open("a", encoding="utf-8") as f:
        f.write(f"{time.time()}: {message}\n")


def _load_catalog(data_dir: Path) -> List[Dict[str, Any]]:
    path = data_dir / "payload_catalog.json"
    if not path.is_file():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


# ── Replay step ───────────────────────────────────────────────────────────────

def do_replay(data_dir: Path, *, workers: int) -> List[Dict[str, Any]]:
    """Replay all payloads in parallel using ThreadPoolExecutor."""
    catalog = _load_catalog(data_dir)
    if not catalog:
        log_error(data_dir, "replay skipped: empty catalog")
        return []

    auth_state = data_dir / "auth_state.json"
    auth_path = auth_state if auth_state.is_file() else None

    from replay import replay_payload_file

    results: List[Dict[str, Any]] = []

    def work(entry: Dict[str, Any]) -> Dict[str, Any]:
        rel_p = entry.get("payload")
        rel_d = entry.get("dump")
        if not rel_p or not rel_d:
            return {"ok": False, "error": "missing paths"}
        payload_path = data_dir / rel_p
        dump_path = data_dir / rel_d
        if not payload_path.is_file():
            return {"ok": False, "error": f"missing payload {payload_path}"}
        try:
            return replay_payload_file(
                payload_path,
                dump_path,
                auth_state_path=auth_path,
                erp_url=entry.get("url"),
            )
        except Exception as e:
            log_error(data_dir, f"replay {payload_path}: {e}")
            return {"ok": False, "error": str(e)}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(work, e): e for e in catalog}
        for fut in as_completed(futures):
            results.append(fut.result())

    ok = sum(1 for r in results if r.get("ok"))
    print(f"[replay] {ok}/{len(results)} succeeded.")

    summary_path = data_dir / "logs" / "replay_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    return results


# ── Parse step ────────────────────────────────────────────────────────────────

def do_parse(data_dir: Path, *, workers: int) -> List[Path]:
    """Parse all detail dump files in parallel."""
    dumps_dir = data_dir / "dumps"
    parsed_dir = data_dir / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Build stem → indent_id lookup from catalog
    id_lookup: Dict[str, Optional[str]] = {}
    for entry in _load_catalog(data_dir):
        if entry.get("method") == _DETAIL_METHOD:
            stem = Path(entry["dump"]).stem
            id_lookup[stem] = entry.get("indent_id")

    # Process only detail dump files (skip list dumps to avoid mixing)
    dump_files = [
        f for f in sorted(dumps_dir.glob("*.txt"))
        if f.stem in id_lookup or any(
            Path(e["dump"]).stem == f.stem
            for e in _load_catalog(data_dir)
            if e.get("method") == _DETAIL_METHOD
        )
    ]

    # If no detail dumps yet, fallback to all dumps
    if not dump_files:
        dump_files = sorted(dumps_dir.glob("*.txt"))

    written: List[Path] = []

    def work(df: Path) -> Optional[Path]:
        out = parsed_dir / f"{df.stem}.json"
        indent_id = id_lookup.get(df.stem)
        try:
            doc = parse_detail_dump_file(df, indent_id=indent_id)
            # Skip if no parts were found (might be a list dump)
            if not doc.get("parts") and doc.get("meta", {}).get("warnings"):
                return None
            out.write_text(
                json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            part_count = len(doc.get("parts", []))
            print(f"  [parse] {df.name} → {part_count} part(s)")
            return out
        except Exception as e:
            log_error(data_dir, f"parse {df}: {e}")
            print(f"  [parse] ERROR {df.name}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for res in ex.map(work, dump_files):
            if res is not None:
                written.append(res)

    print(f"[parse] {len(written)} file(s) written to {parsed_dir}")
    return written


def do_parse_list(data_dir: Path, *, workers: int) -> List[Path]:
    """Parse all list dump files (summary extraction)."""
    dumps_dir = data_dir / "dumps"
    parsed_dir = data_dir / "parsed_list"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Only list dumps
    list_stems: set[str] = {
        Path(e["dump"]).stem
        for e in _load_catalog(data_dir)
        if e.get("method") == _LIST_METHOD
    }
    dump_files = [
        f for f in sorted(dumps_dir.glob("*.txt"))
        if (not list_stems) or f.stem in list_stems
    ]

    written: List[Path] = []

    def work(df: Path) -> Optional[Path]:
        out = parsed_dir / f"{df.stem}.json"
        try:
            doc = parse_list_dump_file(df)
            out.write_text(
                json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            ic = doc.get("meta", {}).get("indent_id_count", 0)
            print(f"  [parse-list] {df.name} → {ic} indent IDs")
            return out
        except Exception as e:
            log_error(data_dir, f"parse-list {df}: {e}")
            print(f"  [parse-list] ERROR {df.name}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for res in ex.map(work, dump_files):
            if res is not None:
                written.append(res)

    return written


# ── Aggregate step ────────────────────────────────────────────────────────────

def do_aggregate(data_dir: Path) -> Path:
    parsed_dir = data_dir / "parsed"
    files = sorted(parsed_dir.glob("*.json"))
    bundle = aggregate_indent_parts(files)
    out_path = data_dir / "output.json"
    out_path.write_text(
        json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"[aggregate] {bundle['total']} part(s) → {out_path}")
    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Promasch Indent Completed extraction pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # Mode flags
    p.add_argument(
        "--collect", action="store_true",
        help="Run Playwright collector (captures indent API calls)",
    )
    p.add_argument(
        "--replay", action="store_true",
        help="Replay all payloads from payload_catalog.json",
    )
    p.add_argument(
        "--parse", action="store_true",
        help="Parse all detail dumps/*.txt → parsed/*.json",
    )
    p.add_argument(
        "--parse-list", action="store_true",
        help="Parse list dumps → parsed_list/*.json (summary only)",
    )
    p.add_argument(
        "--all", action="store_true",
        help="collect → replay → parse → aggregate",
    )
    p.add_argument(
        "--build-detail-payloads", action="store_true",
        help="Build detail request payloads from captured list responses",
    )

    # Data directory
    p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Run directory. Default: data/<ts> for --all/--collect",
    )

    # Parallelism
    p.add_argument("--workers", type=int, default=5)

    # Collector options
    p.add_argument("--base-url", default="https://gw.promasch.in")
    p.add_argument(
        "--user",
        default=os.getenv("INDENT_USER", os.getenv("CONSTRUCTIONARY_USER", "Vikram@greenwave.ws")),
    )
    p.add_argument(
        "--password",
        default=os.getenv(
            "INDENT_PASSWORD", os.getenv("CONSTRUCTIONARY_PASSWORD", "Infosys@9009")
        ),
    )
    p.add_argument("--headful", action="store_true")
    p.add_argument(
        "--wait",
        type=int,
        default=60,
        metavar="SECONDS",
        help="Seconds to wait for API calls during Playwright phase",
    )
    p.add_argument(
        "--no-paginate",
        action="store_true",
        help="Skip automatic list pagination",
    )
    p.add_argument(
        "--page-size",
        type=int,
        default=100,
    )
    p.add_argument(
        "--permutation2",
        default="",
        metavar="HASH",
        help="ERPService2 permutation hash (for --build-detail-payloads)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    do_all = args.all
    run_collect = args.collect or do_all
    run_replay = args.replay or do_all
    run_parse = args.parse or do_all
    run_parse_list = args.parse_list
    run_build = args.build_detail_payloads

    if not any([run_collect, run_replay, run_parse, run_parse_list, run_build]):
        raise SystemExit(
            "Specify at least one mode: "
            "--collect | --replay | --parse | --parse-list | "
            "--build-detail-payloads | --all\n"
            "Use --help for full options."
        )

    data_dir = args.data_dir
    if data_dir is None:
        if run_collect or do_all:
            data_dir = ROOT / "data" / str(int(time.time()))
        else:
            raise SystemExit(
                "--data-dir is required unless --collect or --all is used."
            )

    data_dir = data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    # ── Collect ───────────────────────────────────────────────────────────────
    if run_collect:
        ensure_credentials(args.user, args.password)
        run_collection(
            output_dir=data_dir,
            base_url=args.base_url,
            user=args.user,
            password=args.password,
            headful=args.headful,
            wait_seconds=args.wait,
            auto_paginate=not args.no_paginate,
            page_size=args.page_size,
        )

    # ── Build detail payloads ─────────────────────────────────────────────────
    perm2 = args.permutation2
    if not perm2:
        perm_path = data_dir / "permutation2.txt"
        if perm_path.is_file():
            perm2 = perm_path.read_text(encoding="utf-8").strip()
            print(f"[main] Auto-detected permutation2 from capture: {perm2[:8]}...")

    if run_build or (do_all and perm2):
        if not perm2:
            raise SystemExit(
                "--permutation2 is required. "
                "Find it in any captured detail request payload "
                "(2nd pipe-delimited segment after the base URL)."
            )
        print(f"[main] Building detail payloads (permutation2={perm2[:8]}...)...")
        build_detail_payloads_from_catalog(data_dir, perm2)
    elif do_all:
        print(
            "[main] WARNING: permutation2 not captured from browser. "
            "Re-run with --build-detail-payloads --permutation2 HASH to get detail data."
        )

    # ── Replay ────────────────────────────────────────────────────────────────
    if run_replay:
        print(f"[main] replay → {data_dir}")
        do_replay(data_dir, workers=args.workers)

    # ── Parse list ────────────────────────────────────────────────────────────
    if run_parse_list:
        print(f"[main] parse-list → {data_dir}")
        do_parse_list(data_dir, workers=args.workers)

    # ── Parse detail ──────────────────────────────────────────────────────────
    if run_parse:
        print(f"[main] parse → {data_dir}")
        written = do_parse(data_dir, workers=args.workers)
        if written or (data_dir / "parsed").exists():
            do_aggregate(data_dir)


if __name__ == "__main__":
    main()
