--- description: Get Helix RUM data prepared for AEM reporting
--- Authorization: none
--- earliest: 2021-09-30:13:00:00
--- latest: 2021-09-30:14:00:00
WITH mapping AS
 (SELECT 'www.adobe.com' as host, "express" as applicationID, "helix" AS environmentID, "1" AS releaseID, "helix" AS tier, "rum" AS sourceType, "prod" AS envType, PARSE_TIMESTAMP("%Y-%m-%d:%T","2020-09-30:14:00:00") AS startTime, PARSE_TIMESTAMP("%Y-%m-%d:%T","2021-09-30:13:30:00") AS endTime UNION ALL
  SELECT 'www.adobe.com', "express", "helix", "2", "helix", "rum", "prod", PARSE_TIMESTAMP("%Y-%m-%d:%T","2021-09-30:13:30:00"), NULL UNION ALL
  SELECT 'pages.adobe.com', "adobe pages", "helix", "1", "helix", "rum", "prod", NULL, NULL UNION ALL
  SELECT 'blog.adobe.com', "theblog", "helix", "1", "helix", "rum", "prod", NULL, NULL),
data AS (
SELECT
    applicationID,
    environmentID,
    releaseID,
    tier,
    sourceType,
    envType,
    REGEXP_EXTRACT(MAX(url), r"https://([^/]+)", 1) AS host,
    MAX(weight) as count, 
    MAX(url) as url, 
    MAX(TIMESTAMP_MILLIS(CAST(time AS INT64))) AS time,
    id
FROM `helix-225321.helix_rum.rum202109` LEFT JOIN mapping
ON (
    mapping.host = REGEXP_EXTRACT(url, r"https://([^/]+)", 1)
)
WHERE 
        TIMESTAMP_MILLIS(CAST(time AS INT64)) < PARSE_TIMESTAMP("%Y-%m-%d:%T", @latest)
    AND TIMESTAMP_MILLIS(CAST(time AS INT64)) > PARSE_TIMESTAMP("%Y-%m-%d:%T", @earliest)
    AND (mapping.startTime IS NULL OR mapping.startTime < TIMESTAMP_MILLIS(CAST(time AS INT64)))
    AND (mapping.endTime IS NULL OR mapping.endTime > TIMESTAMP_MILLIS(CAST(time AS INT64)))
GROUP BY 
    applicationID,
    environmentID,
    releaseID,
    tier,
    sourceType,
    envType,
    id
ORDER BY time DESC)
SELECT applicationID, environmentID, releaseID, tier, sourceType, envType, SUM(count) AS count
FROM data
GROUP BY
    applicationID,
    environmentID,
    releaseID,
    tier,
    sourceType,
    envType


