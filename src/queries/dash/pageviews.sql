--- description: Get Helix RUM data for a given domain or owner/repo combination
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 10
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
WITH pageviews_by_id AS (
  SELECT
    hostname,
    id,
    MAX(weight) AS pageviews
  FROM
    `helix-225321.helix_rum.EVENTS_V4`(
      net.host(@url), @offset, @interval, '-', '-', 'UTC', 'all', @domainkey
    )
  GROUP BY id, hostname
)

SELECT
  hostname,
  SUM(pageviews) AS pageviews
FROM pageviews_by_id
GROUP BY hostname
ORDER BY pageviews DESC
