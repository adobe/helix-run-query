--- description: Show page views for specified domains for specified dates based on extrapolation from RUM data
--- Authorization: none
--- interval: 30
--- offset: 0
--- startdate: 2023-05-01
--- enddate: 2023-06-01
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret

WITH rum AS (
  SELECT
    REGEXP_REPLACE(hostname, r'$www.', '') AS hostname,
    COUNT(DISTINCT id) AS rum_count,
    weight,
    FORMAT_DATE('%F', time) AS date
  FROM helix_rum.EVENTS_V3(
    @url,
    CAST(@offset AS INT64),
    CAST(@interval AS INT64),
    @startdate,
    @enddate,
    @timezone,
    @device,
    @domainkey
  )
  GROUP BY date, weight, hostname
)
SELECT
  a.hostname,
  SUM(a.rum_count * a.weight) AS estimated_pv,
  a.date,
  b.ims_org_id
FROM rum AS rum_data
INNER JOIN `helix_reporting.domain_info` b ON a.hostname = b.domain
AND b.ims_org_id != ''
GROUP BY a.hostname, a.date, b.ims_org_id
ORDER BY a.hostname, a.date
