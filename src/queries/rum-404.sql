--- description: Get popularity data for RUM target attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- domainkey: secret

WITH
current_data AS (
  SELECT
    *,
    TIMESTAMP_TRUNC(time, DAY, @timezone) AS date
  FROM
    helix_rum.CHECKPOINTS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
  WHERE checkpoint = '404'
),

checkpoint_urls AS (
  SELECT
    url,
    source,
    COUNT(DISTINCT id) * MAX(pageviews) AS views
  FROM current_data
  GROUP BY url, checkpoint, source
)

SELECT
  url,
  APPROX_TOP_SUM(source, views, 1)[OFFSET(0)].value AS top_source,
  COUNT(source) AS source_count,
  SUM(views) AS views
FROM checkpoint_urls
GROUP BY url
ORDER BY views DESC -- noqa: PRS
LIMIT CAST(@limit AS INT64)
