"""Safe macOS automation for refreshing a Wind-enabled Excel workbook."""

from __future__ import annotations

import hashlib
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, Sequence


EXCEL_PROCESS_NAME = "Microsoft Excel"


class WindExcelAutomationError(RuntimeError):
    """Raised when the Wind Excel workflow cannot complete safely."""


def _run_command(
    args: Sequence[str],
    *,
    timeout: int = 60,
) -> str:
    result = subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "unknown error").strip()
        raise WindExcelAutomationError(
            f"Command failed ({result.returncode}): {message}"
        )
    return result.stdout.strip()


def _run_osascript(script: str, *arguments: str, timeout: int = 60) -> str:
    return _run_command(
        ["/usr/bin/osascript", "-e", script, *arguments],
        timeout=timeout,
    )


def _excel_is_running() -> bool:
    result = subprocess.run(
        ["/usr/bin/pgrep", "-x", EXCEL_PROCESS_NAME],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _file_fingerprint(path: Path) -> Dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as workbook_file:
        for chunk in iter(lambda: workbook_file.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = path.stat()
    return {
        "size": stat.st_size,
        "modified_ns": stat.st_mtime_ns,
        "sha256": digest.hexdigest(),
    }


_INSPECT_EXCEL = r"""
set outputDelimiter to "|||FDH|||"
tell application id "com.microsoft.Excel"
    if not running then return "stopped"
    set workbookCount to count of workbooks
    if workbookCount is 0 then return "running" & outputDelimiter & "0"
    set activeBook to active workbook
    return "running" & outputDelimiter & workbookCount & outputDelimiter & (name of activeBook) & outputDelimiter & (full name of activeBook) & outputDelimiter & (name of active sheet)
end tell
"""


_REFRESH_ACTIVE_SHEET = r"""
on run argv
    set expectedPath to item 1 of argv
    set outputDelimiter to "|||FDH|||"
    tell application id "com.microsoft.Excel"
        activate
        if (count of workbooks) is not 1 then error "Excel must have exactly one workbook open"
        set targetBook to active workbook
        if (full name of targetBook) is not expectedPath then error "The active workbook is not the configured target"

        set loginValue to run VB macro "checkIsLogin"
        if (loginValue as integer) is not 1 then error "Wind Excel add-in is not logged in"

        set targetSheet to active sheet
        set targetSheetName to name of targetSheet
        set cacheResult to run VB macro "clear_cache_data"
        set enable calculation of targetSheet to false
        set enable calculation of targetSheet to true
        set calculation to calculation automatic

        return (name of targetBook) & outputDelimiter & targetSheetName & outputDelimiter & loginValue & outputDelimiter & cacheResult
    end tell
end run
"""


_SAVE_AND_QUIT = r"""
on run argv
    set expectedPath to item 1 of argv
    tell application id "com.microsoft.Excel"
        if (count of workbooks) is not 1 then error "Excel workbook count changed during refresh"
        set targetBook to active workbook
        if (full name of targetBook) is not expectedPath then error "The active workbook changed during refresh"
        save targetBook
        delay 5
        quit saving no
    end tell
end run
"""


_CLEAN_UP_FAILED_RUN = r"""
on run argv
    set expectedPath to item 1 of argv
    set shouldQuit to item 2 of argv
    tell application id "com.microsoft.Excel"
        if not running then return
        if (count of workbooks) is 1 then
            set targetBook to active workbook
            if (full name of targetBook) is expectedPath then close targetBook saving no
        end if
        if shouldQuit is "true" then quit saving no
    end tell
end run
"""


def _parse_excel_state(raw_state: str) -> Dict[str, Any]:
    fields = raw_state.split("|||FDH|||")
    if not fields or fields[0] == "stopped":
        return {"running": False, "workbook_count": 0}
    count = int(fields[1]) if len(fields) > 1 else 0
    state: Dict[str, Any] = {"running": True, "workbook_count": count}
    if count:
        state.update(
            {
                "workbook_name": fields[2],
                "workbook_path": fields[3],
                "sheet_name": fields[4],
            }
        )
    return state


def _wait_for_excel_exit(
    sleep: Callable[[float], None],
    timeout_seconds: int = 60,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _excel_is_running():
            return
        sleep(2)
    raise WindExcelAutomationError("Microsoft Excel did not exit within 60 seconds")


def refresh_wind_workbook(
    workbook_path: str,
    excel_app_path: str,
    *,
    load_wait_seconds: int = 90,
    refresh_wait_seconds: int = 150,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    """Open, refresh, save, and quit one Wind-enabled workbook on macOS.

    Wind's visible ribbon button calls ``clear_cache_data`` and toggles the
    active worksheet's calculation state.  Calling those same add-in entry
    points avoids brittle screen coordinates and does not require macOS
    Accessibility permission.
    """
    workbook = Path(workbook_path).expanduser().resolve()
    excel_app = Path(excel_app_path).expanduser().resolve()
    if not workbook.is_file():
        raise WindExcelAutomationError(f"Workbook does not exist: {workbook}")
    if not excel_app.is_dir():
        raise WindExcelAutomationError(f"Excel application does not exist: {excel_app}")
    if load_wait_seconds < 0 or refresh_wait_seconds < 0:
        raise ValueError("wait durations cannot be negative")

    before = _file_fingerprint(workbook)
    initial_state = _parse_excel_state(_run_osascript(_INSPECT_EXCEL))
    if initial_state["workbook_count"]:
        initial_path = Path(str(initial_state.get("workbook_path", ""))).resolve()
        if initial_state["workbook_count"] != 1 or initial_path != workbook:
            raise WindExcelAutomationError(
                "Microsoft Excel already has another workbook open; refusing "
                "to interrupt an interactive session"
            )

    opened_by_automation = not initial_state["workbook_count"]
    started_excel = not initial_state["running"]
    try:
        if opened_by_automation:
            _run_command(
                ["/usr/bin/open", "-a", str(excel_app), str(workbook)],
                timeout=30,
            )

        sleep(load_wait_seconds)
        loaded_state = _parse_excel_state(_run_osascript(_INSPECT_EXCEL))
        loaded_path = Path(str(loaded_state.get("workbook_path", ""))).resolve()
        if loaded_state.get("workbook_count") != 1 or loaded_path != workbook:
            raise WindExcelAutomationError(
                "The configured workbook was not uniquely active after the load wait"
            )

        refresh_output = _run_osascript(
            _REFRESH_ACTIVE_SHEET,
            str(workbook),
            timeout=120,
        )
        refresh_fields = refresh_output.split("|||FDH|||")
        if len(refresh_fields) < 4:
            raise WindExcelAutomationError(
                f"Unexpected Wind refresh response: {refresh_output}"
            )

        sleep(refresh_wait_seconds)
        _run_osascript(_SAVE_AND_QUIT, str(workbook), timeout=120)
        _wait_for_excel_exit(sleep)
        after = _file_fingerprint(workbook)
    except Exception:
        if opened_by_automation and _excel_is_running():
            try:
                _run_osascript(
                    _CLEAN_UP_FAILED_RUN,
                    str(workbook),
                    str(started_excel).lower(),
                    timeout=60,
                )
                if started_excel:
                    _wait_for_excel_exit(sleep, timeout_seconds=30)
            except Exception:
                pass
        raise

    return {
        "workbook": str(workbook),
        "sheet": refresh_fields[1],
        "wind_login": int(refresh_fields[2]),
        "cache_refresh_result": int(refresh_fields[3]),
        "before": before,
        "after": after,
        "saved": after["modified_ns"] >= before["modified_ns"],
        "excel_exited": True,
        "records_processed": 1,
    }
