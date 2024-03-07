--- description: Get form view, submission, and core web vitals for Forms domains
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
),

view_urls AS (
  SELECT
    url,
    checkpoint,
    source,
    COUNT(id) AS ids,
    COUNT(DISTINCT id) * MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_data
  WHERE
    checkpoint = 'viewblock'
    AND source = '.form'
  GROUP BY url, checkpoint, source
),

submission_urls AS (
  SELECT
    url,
    checkpoint,
    source,
    COUNT(id) AS ids,
    COUNT(DISTINCT id) * MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_data
  WHERE
    checkpoint = 'formsubmit'
    AND (source = '.form' OR source = 'mktoForm')
  GROUP BY url, checkpoint, source
)

SELECT
  v.ids,
  v.views,
  s.actions AS submissions,
  v.actions,
  v.url,
  v.checkpoint,
  v.source,
  v.actions / v.views AS actions_per_view
FROM view_urls AS v
LEFT JOIN submission_urls AS s ON v.url = s.url
ORDER BY v.views DESC
LIMIT CAST(@limit AS INT64) -- noqa: PRS
