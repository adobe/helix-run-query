--- description: Get popularity data for RUM target attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- url: 
--- checkpoint: -
--- source: -

WITH
current_data AS (
    SELECT
        *,
        TIMESTAMP_TRUNC(time, DAY) AS date
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

checkpoint_urls AS (
    SELECT
        url,
        checkpoint,
        source,
        COUNT(id) AS ids,
        MAX(pageviews) AS views,
        SUM(pageviews) AS actions
    FROM current_data
    WHERE (checkpoint = @checkpoint OR @checkpoint = '-')
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
LIMIT @limit
