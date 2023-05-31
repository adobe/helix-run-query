--- description: Show number of Sidekick actions in a time period
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=86400
--- interval: 30
--- offset: 0
--- startdate: 2023-05-01
--- enddate: 2023-05-30
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret

WITH validkeys AS (
  SELECT readonly
  FROM `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE('UTC'))
    AND not readonly
), 
sidekick_events AS (
SELECT
  readonly,
  DATE_TRUNC(time, DAY) AS day,
  id,
  checkpoint,
  SPLIT(SPLIT(hostname, ".")[OFFSET(0)], "--")[OFFSET(1)] AS repo,
  SPLIT(SPLIT(hostname, ".")[OFFSET(0)], "--")[OFFSET(2)] AS owner,
  CONCAT(
    SPLIT(SPLIT(hostname, ".")[OFFSET(0)], "--")[OFFSET(2)],
    "-",
    SPLIT(SPLIT(hostname, ".")[OFFSET(0)], "--")[OFFSET(1)]
  ) AS owner_repo
  #CAST(COUNT(DISTINCT id) * AVG(pageviews) AS INT64) AS actions,
  #APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
  #APPROX_TOP_COUNT(hostname, 1)[OFFSET(0)].value AS tophost,
  #COUNT(DISTINCT hostname) AS hosts,
FROM `helix-225321.helix_rum`.CLUSTER_CHECKPOINTS(
  @url,
  CAST(@offset AS INT64),
  CAST(@interval AS INT64),
  @startdate,
  @enddate,
  @timezone,
  "all",
  "-"
), validkeys
WHERE 
  checkpoint LIKE "sidekick:%" AND
  hostname LIKE "%--%--%.hlx.%" AND 
  array_length(SPLIT(SPLIT(hostname, ".")[OFFSET(0)], "--")) > 2 AND
  not readonly)
SELECT
  checkpoint,
  owner_repo,
  COUNT(*) AS actions,
FROM sidekick_events
GROUP BY owner_repo, checkpoint
ORDER BY owner_repo, actions DESC