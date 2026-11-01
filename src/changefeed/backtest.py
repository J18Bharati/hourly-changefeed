"""M10: a look-ahead backtest on the consumer database.

A hiring signal for a board at time t is computed three ways:

- point-in-time: postings_as_of(t, t), what the change feed had told us by t;
- hindsight: postings_as_of(t, latest), what was true at t as we know it now;
- naive: the postings open now whose published_at is at or before t, which is what a backtest
  built on a "latest" snapshot sees. It is the usual way such a series gets made, and it is wrong
  in a known direction: postings that closed between t and now are missing (survivorship), and
  postings learned after t are present.

The gap between point-in-time and naive is split into its causes:

- survivorship: open at t (hindsight), not open now;
- late_deletions: believed open at t, but closed by t as we learned later;
- late_additions: open at t, but first recorded after t.

The forward test asks which signal (new postings in the window before t) tracks what happened
next (the change in hindsight open postings over the horizon after t). A naive signal can look
predictive because it already knows which postings survived. With fewer than MIN_PAIRS
(board, t) pairs, no correlation is computed.

Boards that stopped being fetched keep their postings open, here as in every as_of query; the
backtest does not try to correct for that.
"""
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

MIN_PAIRS = 30

_POINTS = """
    WITH pit AS (SELECT posting_id FROM postings_as_of(%(t)s, %(t)s)),
         hind AS (SELECT posting_id FROM postings_as_of(%(t)s, %(latest)s)),
         now_open AS (SELECT posting_id, fields FROM postings_as_of(%(latest)s, %(latest)s)),
         first_known AS (SELECT posting_id, min(valid_from) AS opened FROM field_versions
                         WHERE field = 'present' AND value = 'true'::jsonb AND recorded_at <= %(t)s
                         GROUP BY posting_id),
         flags AS (
            SELECT p.source, p.board,
                   pit.posting_id IS NOT NULL AS pit,
                   hind.posting_id IS NOT NULL AS hind,
                   n.posting_id IS NOT NULL AS now_open,
                   (n.fields ->> 'published_at')::timestamptz AS published,
                   f.opened
            FROM postings p
            LEFT JOIN pit ON pit.posting_id = p.id
            LEFT JOIN hind ON hind.posting_id = p.id
            LEFT JOIN now_open n ON n.posting_id = p.id
            LEFT JOIN first_known f ON f.posting_id = p.id)
    SELECT source, board,
           count(*) FILTER (WHERE pit),
           count(*) FILTER (WHERE hind),
           count(*) FILTER (WHERE now_open AND published <= %(t)s),
           count(*) FILTER (WHERE now_open AND published IS NULL),
           count(*) FILTER (WHERE hind AND NOT now_open),
           count(*) FILTER (WHERE pit AND NOT hind),
           count(*) FILTER (WHERE hind AND NOT pit),
           count(*) FILTER (WHERE opened > %(since)s AND opened <= %(t)s),
           count(*) FILTER (WHERE now_open AND published > %(since)s AND published <= %(t)s)
    FROM flags
    WHERE pit OR hind OR now_open
    GROUP BY source, board
    ORDER BY source, board"""


@dataclass(frozen=True)
class Point:
    source: str
    board: str
    at: datetime
    pit_open: int
    hindsight_open: int
    naive_open: int
    naive_undated: int  # open now with no published_at: the naive series cannot place them
    survivorship: int
    late_deletions: int
    late_additions: int
    pit_new: int        # first recorded by t, opened in the window before t
    naive_new: int      # open now, published in the window before t


def first_record(conn) -> datetime | None:
    return conn.execute("SELECT min(recorded_at) FROM field_versions").fetchone()[0]


def last_record(conn) -> datetime | None:
    return conn.execute("SELECT max(recorded_at) FROM field_versions").fetchone()[0]


def points(conn, at: datetime, window: timedelta, latest: datetime) -> list[Point]:
    """One point per board with any posting open at `at` (by any of the three views). Before the
    first record there is nothing to compare: the point-in-time view knew nothing."""
    first = first_record(conn)
    if first is None or at < first:
        return []
    rows = conn.execute(_POINTS, {"t": at, "latest": latest, "since": at - window}).fetchall()
    return [Point(s, b, at, *counts) for s, b, *counts in rows]


@dataclass
class Summary:
    start: datetime
    end: datetime
    latest: datetime
    window: timedelta
    horizon: timedelta
    points: int = 0
    pit_open: int = 0
    hindsight_open: int = 0
    naive_open: int = 0
    naive_undated: int = 0
    survivorship: int = 0
    late_deletions: int = 0
    late_additions: int = 0
    pit_new: int = 0
    naive_new: int = 0
    new_disagree: int = 0  # points where the two signals differ
    forward_pairs: int = 0
    corr_pit: float | None = None
    corr_naive: float | None = None
    worst: list[tuple[str, int]] = field(default_factory=list)  # boards by survivorship


def _corr(xs, ys) -> float | None:
    try:
        return statistics.correlation(xs, ys)
    except statistics.StatisticsError:  # too few, or a constant series
        return None


def run(conn, start: datetime, end: datetime, step: timedelta, window: timedelta,
        horizon: timedelta, latest: datetime) -> Summary:
    s = Summary(start, end, latest, window, horizon)
    cache: dict[datetime, dict[tuple[str, str], Point]] = {}

    def at(t):
        if t not in cache:
            cache[t] = {(p.source, p.board): p for p in points(conn, t, window, latest)}
        return cache[t]

    by_board: dict[str, int] = {}
    signal_pit, signal_naive, outcome = [], [], []
    t = start
    while t <= end:
        for key, p in at(t).items():
            s.points += 1
            for name in ("pit_open", "hindsight_open", "naive_open", "naive_undated", "survivorship",
                         "late_deletions", "late_additions", "pit_new", "naive_new"):
                setattr(s, name, getattr(s, name) + getattr(p, name))
            s.new_disagree += p.pit_new != p.naive_new
            by_board[f"{p.source}/{p.board}"] = by_board.get(f"{p.source}/{p.board}", 0) + p.survivorship
            if t + horizon <= latest:
                later = at(t + horizon).get(key)
                signal_pit.append(p.pit_new)
                signal_naive.append(p.naive_new)
                outcome.append((later.hindsight_open if later else 0) - p.hindsight_open)
        t += step
    s.forward_pairs = len(outcome)
    if s.forward_pairs >= MIN_PAIRS:
        s.corr_pit, s.corr_naive = _corr(signal_pit, outcome), _corr(signal_naive, outcome)
    s.worst = sorted(((b, n) for b, n in by_board.items() if n), key=lambda x: (-x[1], x[0]))[:5]
    return s


def _pct(part, whole) -> str:
    return f"{100 * part / whole:.1f}%" if whole else "-"


def _r(x) -> str:
    return "-" if x is None else f"{x:+.2f}"


def render(s: Summary) -> str:
    utc = lambda t: f"{t.astimezone(timezone.utc):%Y-%m-%d %H:%M}Z"  # noqa: E731
    lines = [
        f"backtest {utc(s.start)} .. {utc(s.end)}, latest record {utc(s.latest)}; "
        f"window {s.window}, horizon {s.horizon}; {s.points} (board, time) points",
        "open postings, summed over points:",
        f"  point-in-time {s.pit_open}, hindsight {s.hindsight_open}, naive (latest records) {s.naive_open}"
        f" ({_pct(s.naive_open - s.hindsight_open, s.hindsight_open)} against hindsight)",
        f"  survivorship (open then, closed since) {s.survivorship} ({_pct(s.survivorship, s.hindsight_open)})",
        f"  late deletions (closed by then, learned after) {s.late_deletions}",
        f"  late additions (open by then, learned after) {s.late_additions}",
        f"  open now without published_at (naive cannot place) {s.naive_undated}",
        f"new postings in the window: point-in-time {s.pit_new}, naive {s.naive_new}; "
        f"the two differ at {s.new_disagree} of {s.points} points",
    ]
    if s.forward_pairs >= MIN_PAIRS and (s.corr_pit is not None or s.corr_naive is not None):
        lines.append(f"forward test over {s.forward_pairs} pairs: corr(signal, change in open postings "
                     f"over the horizon) point-in-time {_r(s.corr_pit)}, naive {_r(s.corr_naive)}")
    else:
        lines.append(f"forward test: not enough history ({s.forward_pairs} pairs with a full horizon, "
                     f"{MIN_PAIRS} needed, or a constant series)")
    if s.worst:
        lines.append("most survivorship: " + ", ".join(f"{b} {n}" for b, n in s.worst))
    return "\n".join(lines)
