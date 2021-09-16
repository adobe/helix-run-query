--- description: Get daily traffic, broken down by generation
--- Authorization: none
--- interval: 7
--- timezone: UTC
--- urls: -
--- domain: -
--- generations: -


EXECUTE IMMEDIATE CONCAT("""
WITH by_generation AS (
SELECT 
    id,
    generation,
    IF(url IN UNNEST(SPLIT(@urls, ",")), url, "other") AS url,
    MAX(weight) AS weight,
    MAX(time) AS last,
    MIN(time) AS first,
    FORMAT_TIMESTAMP( '%y-%m-%d', TIMESTAMP_MILLIS(CAST(time AS INT64)), @timezone) AS date
FROM `helix-225321.helix_rum.rum202109` 
WHERE 
    generation IS NOT NULL
    AND url LIKE CONCAT("https://", @domain, "%")
    AND CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@intervaldays AS INT64) DAY)) AS STRING)
GROUP BY id, generation, url, date),
results AS (
SELECT 
    date,
    url,
    generation,
    SUM(weight) AS pageviews,
    IF((MAX(last) - MIN(first)) / 1000 / 3600 > 0, ROUND(SUM(weight) / ((MAX(last) - MIN(first)) / 1000 / 3600)), 0) AS pageviews_per_hour,
    (MAX(last) - MIN(first)) / 1000 / 3600 AS hours,
FROM by_generation
GROUP BY generation, url, date
ORDER BY 
    date DESC,
    url ASC,
    generation ASC
)
SELECT 
    date, 
    url, 
""",
ARRAY_TO_STRING(ARRAY(SELECT 
    CONCAT('SUM(pageviews * IF(generation = "', generation, '", 1, 0)) AS ', REPLACE(generation, '-', '_')) AS expr 
    FROM UNNEST(SPLIT(@generations, ',')) AS generation
),', '),
    # SUM(pageviews * IF(generation = "instrumentation-test", 1, 0)) AS instrumentation_test,
    # SUM(pageviews * IF(generation = "instrumentation-test-sync", 1, 0)) AS instrumentation_test_sync,
    # SUM(pageviews * IF(generation = "instrumentation-test-martech", 1, 0)) AS instrumentation_test_martech 
"""
FROM results
GROUP BY date, url
ORDER BY 
    date DESC,
    url ASC
""") USING 
  @domain AS domain, 
  @interval AS intervaldays, 
  @timezone AS timezone, 
  @urls AS urls;