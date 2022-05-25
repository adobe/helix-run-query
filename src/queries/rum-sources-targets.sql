--- description: Get popularity data for RUM source attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- url: -
--- checkpoint: -
--- source: -

WITH
current_data AS (
  SELECT
    *
  FROM helix_rum.CLUSTER_CHECKPOINTS(
    @url,
    CAST(@offset AS INT64),
    CAST(@interval AS INT64),
    '2022-01-01',
    '2022-01-31',
    'UTC',
    'all',
    '-'
  )
),

sources AS (
  SELECT
    id,
    source,
    checkpoint,
    target,
    ANY_VALUE(user_agent) AS user_agent,
    ANY_VALUE(url) AS url,
    MAX(pageviews) AS views,
    SUM(pageviews) AS actions
  FROM current_data
  WHERE source IS NOT NULL
    AND (@source = '-' OR source = @source)
    AND (@checkpoint = '-' OR @checkpoint = checkpoint)
  GROUP BY source, id, checkpoint, target
)

SELECT
  checkpoint,
  source,
  target,
  COUNT(id) AS ids,
  COUNT(DISTINCT url) AS pages,
  APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
  COUNT(DISTINCT user_agent) AS user_agents,
  APPROX_TOP_COUNT(user_agent, 1)[OFFSET(0)].value AS top_user_agent,
  SUM(views) AS views,
  SUM(actions) AS actions
FROM sources
GROUP BY source, checkpoint, target
ORDER BY views DESC
LIMIT @limit
