--- description: Get popularity data for RUM source attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- checkpoint: -
--- source: -
--- aggregate: true
--- domainkey: secret

WITH current_data AS (
  SELECT *
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

sources AS (
  SELECT
    id,
    source,
    checkpoint,
    MAX(url) AS url,
    MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_data
  WHERE
    source IS NOT NULL AND (
      CAST(
        @checkpoint AS STRING
      ) = '-' OR CAST(@checkpoint AS STRING) = checkpoint
    ) AND (source = @source OR @source = '-')
  GROUP BY source, id, checkpoint
),

aggregate_true AS (
  SELECT
    checkpoint,
    source,
    'true' AS aggregate,
    '' AS url,
    APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
    COUNT(id) AS ids,
    COUNT(DISTINCT url) AS pages,
    SUM(views) AS views,
    SUM(actions) AS actions,
    SUM(actions) / SUM(views) AS actions_per_view
  FROM sources
  GROUP BY source, checkpoint
),

aggregate_false AS (
  SELECT
    checkpoint,
    source,
    'false' AS aggregate,
    url,
    '' AS topurl,
    COUNT(id) AS ids,
    COUNT(DISTINCT url) AS pages,
    SUM(views) AS views,
    SUM(actions) AS actions,
    SUM(actions) / SUM(views) AS actions_per_view
  FROM sources
  GROUP BY source, checkpoint, url
),

aggregate_all AS (
  SELECT
    checkpoint,
    source,
    ids,
    pages,
    url,
    topurl,
    views,
    actions,
    actions_per_view,
    aggregate
  FROM aggregate_true
  UNION ALL
  SELECT
    checkpoint,
    source,
    ids,
    pages,
    url,
    topurl,
    views,
    actions,
    actions_per_view,
    aggregate
  FROM aggregate_false
)

SELECT
  checkpoint,
  source,
  ids,
  pages,
  url,
  topurl,
  views,
  actions,
  actions_per_view
FROM aggregate_all
WHERE aggregate = CAST(@aggregate AS STRING)
ORDER BY views DESC
LIMIT @limit
