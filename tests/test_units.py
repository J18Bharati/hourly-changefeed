"""The systemd units pass systemd-analyze without warnings (it caught RuntimeMaxSec on a oneshot)."""
import shutil
import subprocess
from pathlib import Path

import pytest

UNITS = sorted((Path(__file__).parent.parent / "deploy" / "systemd").iterdir())


@pytest.mark.skipif(not shutil.which("systemd-analyze"), reason="needs systemd")
def test_units_verify_cleanly(tmp_path):
    # Verify copies whose ExecStart points at an executable that exists, so only real warnings remain.
    for unit in UNITS:
        text = unit.read_text().replace("%h/.local/state/changefeed/result/bin/changefeed", "/bin/true") \
                               .replace("%h/.local/state/changefeed/postgres/bin/postgres", "/bin/true")
        (tmp_path / unit.name).write_text(text)
    out = subprocess.run(["systemd-analyze", "--user", "verify", *sorted(map(str, tmp_path.iterdir()))],
                         capture_output=True, text=True)
    ours = [l for l in (out.stdout + out.stderr).splitlines() if "changefeed" in l]
    assert out.returncode == 0 and ours == [], ours
