#!/usr/bin/env python3
"""Install the FinanceDataHub macOS desktop worker as a LaunchAgent."""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import time
from pathlib import Path


LABEL = "com.tradingnexus.financedatahub.desktop-automation"


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    python_path = project_root / ".venv" / "bin" / "python"
    if not python_path.is_file():
        raise SystemExit(
            f"FinanceDataHub virtual environment is missing: {python_path}"
        )

    service_root = Path.home() / ".financedatahub" / "desktop_automation"
    runtime_dir = service_root / "runtime"
    queue_root = service_root / "queue"
    log_dir = service_root / "logs"
    for directory in (runtime_dir, queue_root, log_dir):
        directory.mkdir(parents=True, exist_ok=True)

    source_dir = project_root / "finance_data_hub" / "desktop_automation"
    worker_path = runtime_dir / "worker.py"
    shutil.copy2(source_dir / "worker.py", worker_path)
    shutil.copy2(source_dir / "wind_excel.py", runtime_dir / "wind_excel.py")

    launch_agents = Path.home() / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True, exist_ok=True)
    plist_path = launch_agents / f"{LABEL}.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": [
            str(python_path.resolve()),
            str(worker_path),
            "--queue-root",
            str(queue_root),
        ],
        "WorkingDirectory": str(runtime_dir),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "StandardOutPath": str(log_dir / "worker.log"),
        "StandardErrorPath": str(log_dir / "worker.error.log"),
    }
    with plist_path.open("wb") as plist_file:
        plistlib.dump(payload, plist_file, sort_keys=True)

    domain = f"gui/{os.getuid()}"
    subprocess.run(
        ["/bin/launchctl", "bootout", domain, str(plist_path)],
        capture_output=True,
        text=True,
    )
    for _ in range(50):
        current = subprocess.run(
            ["/bin/launchctl", "print", f"{domain}/{LABEL}"],
            capture_output=True,
            text=True,
        )
        if current.returncode != 0:
            break
        time.sleep(0.1)

    bootstrap = subprocess.run(["/bin/launchctl", "bootstrap", domain, str(plist_path)])
    if bootstrap.returncode != 0:
        time.sleep(1)
        subprocess.run(
            ["/bin/launchctl", "bootstrap", domain, str(plist_path)],
            check=True,
        )
    subprocess.run(
        ["/bin/launchctl", "kickstart", "-k", f"{domain}/{LABEL}"],
        check=True,
    )
    print(f"Installed and started {LABEL}")
    print(f"Queue: {queue_root}")
    print(f"LaunchAgent: {plist_path}")


if __name__ == "__main__":
    main()
