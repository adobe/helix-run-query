--- description: Show content reach and persistence over time
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 60
--- offset: 0
--- domain: -
WITH visits AS (
    SELECT
        url,
        id,
        pageviews AS weight,
        TIMESTAMP_TRUNC(time, DAY) AS time
    FROM helix_rum.CLUSTER_PAGEVIEWS(
        @domain,
        CAST(@offset AS INT64),
        CAST(@interval AS INT64) + 1,
        '2022-02-01', # not used, start date
        '2022-05-28', # not used, end date
        'GMT', # timezone
        'all', # device class
        '-' # not used, generation
    )
),

urldays AS (
    # FULL JOIN days ON (days.time = visits.time)
    SELECT
        time,
        url,
        COUNT(id) AS events,
        SUM(weight) AS visits
    FROM visits
    GROUP BY time, url
),

steps AS (
    SELECT
        time,
        url,
        events,
        visits,
        TIMESTAMP_DIFF(
            time, LAG(time) OVER(PARTITION BY url ORDER BY time), DAY
        ) AS step
    FROM urldays
),

chains AS (
    SELECT
        time,
        url,
        events,
        visits,
        steps,
        COUNTIF(step = 1) OVER(PARTITION BY url ORDER BY time) AS chain
    FROM steps
),

urlchains AS (
    SELECT
        url,
        time,
        chain,
        events,
        visits
    FROM chains
    ORDER BY chain DESC
),

powercurve AS (
    SELECT
        MAX(chain) AS persistence,
        COUNT(url) AS reach
    FROM urlchains
    GROUP BY chain
    ORDER BY MAX(chain) ASC
    LIMIT 31 OFFSET 1
)

SELECT * FROM powercurve
