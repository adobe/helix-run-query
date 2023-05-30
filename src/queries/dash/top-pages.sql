--- description: Show page views for specified domains for specified dates based on extrapolation from RUM data
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2023-05-01
--- enddate: 2023-06-01
--- timezone: UTC
--- url: -
--- device: all
--- limit: 100
--- domainkey: secret

WITH rum AS (
  SELECT
    id,
    url,
    weight
  FROM
    helix_rum.EVENTS_V3(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      @device,
      @domainkey
    )
  GROUP BY id, url, weight
)

SELECT
  url,
  COUNT(id) * weight AS estimated_pv
FROM rum
GROUP BY url, weight
ORDER BY estimated_pv DESC
LIMIT @limit
