from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from app.raw_data_store import RawTable


def run_pandas_sandbox_analysis(
    tables: list[RawTable],
    family: str,
    *,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    backend_root = Path(__file__).resolve().parents[1]
    payload = {
        "family": family,
        "tables": [{"name": table.name, "headers": table.headers, "rows": table.rows} for table in tables],
    }
    with tempfile.TemporaryDirectory(prefix="gkm-pandas-sandbox-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        input_path = tmp_path / "input.json"
        output_path = tmp_path / "output.json"
        input_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
        command = [
            sys.executable,
            "-m",
            "app.pandas_sandbox_worker",
            str(input_path),
            str(output_path),
        ]
        completed = subprocess.run(
            command,
            cwd=str(backend_root),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
            return {"error": f"pandas sandbox failed: {detail[:400]}"}
        if not output_path.exists():
            return {"error": "pandas sandbox failed: worker did not write an output payload"}
        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return {"error": f"pandas sandbox failed: invalid JSON output ({exc})"}
