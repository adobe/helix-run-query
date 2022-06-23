--- description: Compare historic Google CRUX (Chrome RUM UX) report data
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- domain: -
--- then: 2201
--- now: 2205
WITH lcp_now AS (
  SELECT SUM(lcp.density) AS good_lcp_now
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(largest_contentful_paint.histogram.bin) AS lcp
  WHERE
    origin = CONCAT("https://", @domain)
    AND lcp.start < 2500
    AND _table_suffix = @now
),

lcp_then AS (
  SELECT SUM(lcp.density) AS good_lcp_then
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(largest_contentful_paint.histogram.bin) AS lcp
  WHERE
    origin = CONCAT("https://", @domain)
    AND lcp.start < 2500
    AND _table_suffix = @then
),

cls_now AS (
  SELECT SUM(cls.density) AS good_cls_now
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(layout_instability.cumulative_layout_shift.histogram.bin) AS cls
  WHERE
    origin = CONCAT("https://", @domain)
    AND cls.start < 0.1
    AND _table_suffix = @now
),

cls_then AS (
  SELECT SUM(cls.density) AS good_cls_then
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(layout_instability.cumulative_layout_shift.histogram.bin) AS cls
  WHERE
    origin = CONCAT("https://", @domain)
    AND cls.start < 0.1
    AND _table_suffix = @then
),

fid_now AS (
  SELECT SUM(fid.density) AS good_fid_now
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(first_input.delay.histogram.bin) AS fid
  WHERE
    origin = CONCAT("https://", @domain)
    AND fid.start < 100
    AND _table_suffix = @now
),

fid_then AS (
  SELECT SUM(fid.density) AS good_fid_then
  FROM
    `chrome-ux-report.all.20*`,
    UNNEST(first_input.delay.histogram.bin) AS fid
  WHERE
    origin = CONCAT("https://", @domain)
    AND fid.start < 100
    AND _table_suffix = @then
),

comparison AS (
  SELECT
    cls_now.good_cls_now,
    cls_then.good_cls_then,
    fid_now.good_fid_now,
    fid_then.good_fid_then,
    lcp_now.good_lcp_now,
    lcp_then.good_lcp_then
  FROM lcp_now, cls_now, fid_now, lcp_then, cls_then, fid_then
)

SELECT
  STRUCT(
    ROUND(good_lcp_now * 100) AS now,
    ROUND(good_lcp_then * 100) AS before,
    ROUND(100 * (good_lcp_now - good_lcp_then) / good_lcp_then) AS improvement
  ) AS lcp,
  STRUCT(
    ROUND(good_fid_now * 100) AS now,
    ROUND(good_fid_then * 100) AS before,
    ROUND(100 * (good_fid_now - good_fid_then) / good_fid_then) AS improvement
  ) AS fid,
  STRUCT(
    ROUND(good_cls_now * 100) AS now,
    ROUND(good_cls_then * 100) AS before,
    ROUND(100 * (good_cls_now - good_cls_then) / good_cls_then) AS improvement
  ) AS cls
FROM comparison
