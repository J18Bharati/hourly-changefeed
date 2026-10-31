-- Part B M8: the same question as the producer's postings_as_of, answered from change files alone.
-- `known` is compared with the producer's recorded_at, carried in every row, not with our load
-- time: loading a week of files today must not make today the moment we learned them.
CREATE FUNCTION postings_as_of(valid timestamptz, known timestamptz)
RETURNS TABLE (posting_id bigint, fields jsonb) LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (v.posting_id, v.field) v.posting_id, v.field, v.value
        FROM field_versions v
        WHERE v.valid_from <= valid AND v.recorded_at <= known
        ORDER BY v.posting_id, v.field, v.valid_from DESC)
    SELECT l.posting_id, coalesce(jsonb_object_agg(l.field, l.value) FILTER (WHERE l.field <> 'present'), '{}')
    FROM latest l
    GROUP BY l.posting_id
    HAVING bool_or(l.field = 'present' AND l.value = 'true'::jsonb)
$$;
