--- description: Get popularity data for RUM source attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- url: -
--- checkpoint: -

WITH 
current_data AS (
  SELECT 
    TIMESTAMP_TRUNC(time, DAY) AS date,
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
    MAX(url) AS url, 
    MAX(pageviews) AS views,
    SUM(pageviews) AS actions,
  FROM current_data 
  WHERE source IS NOT NULL AND (@checkpoint = '-' OR CAST(@checkpoint AS STRING) = checkpoint)
  GROUP BY source, id, checkpoint 
)

SELECT 
  COUNT(id) AS ids,
  COUNT(DISTINCT url) AS pages,
  APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
  SUM(views) AS views,
  SUM(actions) AS actions,
  SUM(actions) / SUM(views) AS actions_per_view,
  checkpoint,
  source,
FROM sources
GROUP BY source, checkpoint
ORDER BY views DESC
LIMIT @limit