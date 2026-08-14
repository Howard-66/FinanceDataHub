"""macOS queue worker for FinanceDataHub desktop automation jobs."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

try:
    from .wind_excel import refresh_wind_workbook
except ImportError:  # LaunchAgent runs a self-contained copy outside the package.
    from wind_excel import refresh_wind_workbook


_SHOULD_STOP = False


def _handle_stop(_signum: int, _frame: Any) -> None:
    global _SHOULD_STOP
    _SHOULD_STOP = True


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary_path, path)


def _process_request(request: Dict[str, Any]) -> Dict[str, Any]:
    request_id = str(request.get("request_id", ""))
    action = str(request.get("action", ""))
    params = request.get("params") or {}
    started_at = datetime.now().astimezone()

    result: Dict[str, Any] = {
        "request_id": request_id,
        "job_id": request.get("job_id"),
        "action": action,
        "started_at": started_at.isoformat(),
    }
    try:
        if action != "wind_excel_refresh":
            raise ValueError(f"Unsupported desktop automation action: {action}")
        details = refresh_wind_workbook(
            workbook_path=str(params["workbook_path"]),
            excel_app_path=str(params["excel_app_path"]),
            load_wait_seconds=int(params.get("load_wait_seconds", 90)),
            refresh_wait_seconds=int(params.get("refresh_wait_seconds", 150)),
        )
        result.update({"status": "completed", "details": details})
    except Exception as exc:
        result.update(
            {
                "status": "failed",
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
    result["finished_at"] = datetime.now().astimezone().isoformat()
    return result


def process_next(queue_root: Path) -> bool:
    request_dir = queue_root / "requests"
    processing_dir = queue_root / "processing"
    completed_dir = queue_root / "completed"
    result_dir = queue_root / "results"
    for directory in (request_dir, processing_dir, completed_dir, result_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for request_path in sorted(request_dir.glob("*.json")):
        processing_path = processing_dir / request_path.name
        try:
            os.replace(request_path, processing_path)
        except FileNotFoundError:
            continue

        try:
            request = json.loads(processing_path.read_text(encoding="utf-8"))
            result = _process_request(request)
        except Exception as exc:
            result = {
                "request_id": processing_path.stem,
                "status": "failed",
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "finished_at": datetime.now().astimezone().isoformat(),
            }

        _write_json_atomic(result_dir / processing_path.name, result)
        os.replace(processing_path, completed_dir / processing_path.name)
        status = result.get("status", "failed")
        print(
            f"{datetime.now().isoformat()} request={processing_path.stem} "
            f"status={status}",
            flush=True,
        )
        return True
    return False


def run_worker(queue_root: Path, poll_seconds: float = 2.0) -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    print(
        f"{datetime.now().isoformat()} desktop worker started "
        f"queue_root={queue_root}",
        flush=True,
    )
    while not _SHOULD_STOP:
        if not process_next(queue_root):
            time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-root", required=True, type=Path)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    if sys.platform != "darwin":
        raise SystemExit("The desktop automation worker must run on macOS")
    if args.once:
        process_next(args.queue_root.expanduser().resolve())
        return
    run_worker(
        args.queue_root.expanduser().resolve(),
        poll_seconds=args.poll_seconds,
    )


if __name__ == "__main__":
    main()
