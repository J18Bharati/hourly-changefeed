-- How each board's last fetch in each hour went (manifest format 2), so the consumer can tell a
-- board that stopped being fetched from one that stopped changing.
CREATE TABLE board_observations (
    hour    timestamptz NOT NULL REFERENCES loaded_files (hour),
    source  text NOT NULL,
    board   text NOT NULL,
    outcome text NOT NULL,
    PRIMARY KEY (hour, source, board)
);
CREATE INDEX board_observations_board ON board_observations (source, board, hour);

CREATE INDEX field_versions_hour ON field_versions (hour);
