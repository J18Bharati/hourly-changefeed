"""The systemd units pass systemd-analyze without warnings (it caught RuntimeMaxSec on a oneshot)."""
import shutil
import subprocess
import time
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


def _dns_wait(host: str, tries: int) -> list[str]:
    """The service's ExecStartPre as a shell command, with systemd's $$ unescaped."""
    unit = (Path(__file__).parent.parent / "deploy" / "systemd" / "changefeed.service").read_text()
    pre = [l.removeprefix("ExecStartPre=") for l in unit.splitlines() if l.startswith("ExecStartPre=")]
    assert len(pre) == 1, "changefeed.service waits for DNS before the run"
    cmd = pre[0].replace("$$", "$").replace("seq 60", f"seq {tries}").replace("boards-api.greenhouse.io", host)
    return ["/bin/sh", "-c", cmd.removeprefix("/bin/sh -c '").removesuffix("'")]


def test_the_run_waits_for_dns_but_never_fails_on_it():
    # Live, 2026-11-01: after a laptop sleep the catch-up run fetched 4 s before Wi-Fi was back,
    # and all 201 boards got "Temporary failure in name resolution".
    start = time.monotonic()
    assert subprocess.run(_dns_wait("no-such-host.invalid", 2)).returncode == 0, "an outage is recorded, not hidden"
    assert time.monotonic() - start >= 1.5, "it waited"
    start = time.monotonic()
    assert subprocess.run(_dns_wait("localhost", 60)).returncode == 0
    assert time.monotonic() - start < 1, "no wait once names resolve"
