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
            "refresh_wait_seconds": 150,
        },
    }
    (request_dir / "request-1.json").write_text(
        json.dumps(request, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        worker,
        "refresh_wind_workbook",
        lambda **kwargs: {
            "workbook": kwargs["workbook_path"],
            "saved": True,
            "excel_exited": True,
            "records_processed": 1,
        },
    )

    assert worker.process_next(queue_root) is True

    result = json.loads(
        (queue_root / "results" / "request-1.json").read_text(encoding="utf-8")
    )
    assert result["status"] == "completed"
    assert result["details"]["saved"] is True
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
