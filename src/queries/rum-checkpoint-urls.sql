--- description: Get popularity data for RUM target attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=86400
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- checkpoint: -
--- source: -
--- domainkey: secret

WITH
current_data AS (
  SELECT
    *,
    TIMESTAMP_TRUNC(time, DAY) AS date
  FROM
    helix_rum.CHECKPOINTS_V3(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
),

checkpoint_urls AS (
  SELECT
    url,
    checkpoint,
    source,
    COUNT(id) AS ids,
    COUNT(DISTINCT id) * MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_data
  WHERE
    (
      checkpoint = CAST(@checkpoint AS STRING)
      OR CAST(@checkpoint AS STRING) = '-'
    )
    AND (source = @source OR @source = '-')
  GROUP BY url, checkpoint, source
)

SELECT
  ids,
  views,
  actions,
  url,
  checkpoint,
  source,
  actions / views AS actions_per_view
FROM checkpoint_urls
ORDER BY views DESC
LIMIT CAST(@limit AS INT64)
