CREATE OR REPLACE TEMP TABLE all_results
(
  is_admin BOOLEAN,
  events ARRAY<STRING>,
  metadata STRUCT<foo STRING, bar INT>,
  meta_array ARRAY<STRUCT<foo STRING, bar INT>>
);

INSERT INTO all_results
(is_admin, events, metadata, meta_array)
VALUES
(
  false, -- noqa: PRS
  ARRAY['{"event": "page_view", "url": "/"}',
        '{"event": "page_view", "url": "/about"}',
        '{"event": "page_view", "url": "/contact"}'],
  STRUCT('foo', 2),
  [STRUCT('foo', 2), STRUCT('foo', 3)]
);

# hlx:metadata
SELECT 100 AS total_rows;

SELECT
  is_admin,
  events,
  metadata,
  meta_array
FROM all_results LIMIT 1;
