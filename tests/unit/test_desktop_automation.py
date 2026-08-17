import json


def test_worker_claims_request_and_publishes_result(tmp_path, monkeypatch):
    from finance_data_hub.desktop_automation import worker

    queue_root = tmp_path / "queue"
    request_dir = queue_root / "requests"
    request_dir.mkdir(parents=True)
    request = {
        "request_id": "request-1",
        "job_id": "basisflow_wind_excel_refresh",
        "action": "wind_excel_refresh",
        "params": {
            "workbook_path": "/tmp/煤焦钢矿.xlsx",
            "excel_app_path": "/tmp/Microsoft Excel.app",
            "load_wait_seconds": 90,
            "load_timeout_seconds": 360,
            "refresh_wait_seconds": 150,
        },
    }
    (request_dir / "request-1.json").write_text(
        json.dumps(request, ensure_ascii=False),
        encoding="utf-8",
    )
    received = {}

    def fake_refresh(**kwargs):
        received.update(kwargs)
        return {
            "workbook": kwargs["workbook_path"],
            "saved": True,
            "excel_exited": True,
            "records_processed": 1,
        }

    monkeypatch.setattr(worker, "refresh_wind_workbook", fake_refresh)

    assert worker.process_next(queue_root) is True

    result = json.loads(
        (queue_root / "results" / "request-1.json").read_text(encoding="utf-8")
    )
    assert result["status"] == "completed"
    assert result["details"]["saved"] is True
    assert received["load_timeout_seconds"] == 360
    assert (queue_root / "completed" / "request-1.json").exists()


def test_parse_excel_state_preserves_unicode_path():
    from finance_data_hub.desktop_automation.wind_excel import _parse_excel_state

    state = _parse_excel_state(
        "running|||FDH|||1|||FDH|||煤焦钢矿.xlsx|||FDH|||"
        "/Volumes/Repository/煤焦钢矿.xlsx|||FDH|||螺纹钢"
    )

    assert state["running"] is True
    assert state["workbook_count"] == 1
    assert state["workbook_path"] == "/Volumes/Repository/煤焦钢矿.xlsx"
    assert state["sheet_name"] == "螺纹钢"


def test_worker_skips_request_superseded_by_later_success(tmp_path, monkeypatch):
    from finance_data_hub.desktop_automation import worker

    queue_root = tmp_path / "queue"
    request_dir = queue_root / "requests"
    result_dir = queue_root / "results"
    request_dir.mkdir(parents=True)
    result_dir.mkdir(parents=True)
    request = {
        "request_id": "basisflow-early",
        "job_id": "basisflow",
        "action": "wind_excel_refresh",
        "requested_at": "2026-08-14T21:20:00+08:00",
        "params": {},
    }
    (request_dir / "basisflow-early.json").write_text(
        json.dumps(request), encoding="utf-8"
    )
    (result_dir / "basisflow-later.json").write_text(
        json.dumps(
            {
                "request_id": "basisflow-later",
                "job_id": "basisflow",
                "status": "completed",
                "finished_at": "2026-08-14T21:25:00+08:00",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        worker,
        "refresh_wind_workbook",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not refresh")),
    )

    assert worker.process_next(queue_root) is True

    result = json.loads(
        (result_dir / "basisflow-early.json").read_text(encoding="utf-8")
    )
    assert result["status"] == "completed"
    assert result["details"]["skipped_duplicate"] is True
    assert result["details"]["superseded_by"] == "basisflow-later"


def test_worker_recovers_interrupted_processing_request(tmp_path):
    from finance_data_hub.desktop_automation import worker

    queue_root = tmp_path / "queue"
    processing_dir = queue_root / "processing"
    processing_dir.mkdir(parents=True)
    request = {
        "request_id": "interrupted-1",
        "job_id": "basisflow",
        "action": "wind_excel_refresh",
        "requested_at": "2026-08-14T21:20:00+08:00",
    }
    (processing_dir / "interrupted-1.json").write_text(
        json.dumps(request), encoding="utf-8"
    )

    assert worker.recover_interrupted_requests(queue_root) == 1

    result = json.loads(
        (queue_root / "results" / "interrupted-1.json").read_text(encoding="utf-8")
    )
    assert result["status"] == "failed"
    assert "restarted" in result["error"]
    assert (queue_root / "completed" / "interrupted-1.json").exists()
