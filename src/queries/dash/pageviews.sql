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
--- domainkey: secret

-- TODO: consider whether to retain www prefix and remove IMS org from this query

WITH rum AS (
  SELECT
    weight,
    REGEXP_REPLACE(hostname, r'www.', '') AS hostname,
    COUNT(DISTINCT id) AS ids,
    FORMAT_DATE('%F', time) AS date
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
  GROUP BY date, weight, hostname
)

SELECT
  rum_data.hostname,
  rum_data.date,
  di.ims_org_id,
  rum_data.ids * rum_data.weight AS estimated_pv
FROM rum AS rum_data
INNER JOIN `helix_reporting.domain_info` AS di
  ON
    rum_data.hostname = di.domain
    AND di.ims_org_id != ''
GROUP BY
  rum_data.hostname, rum_data.date, di.ims_org_id, rum_data.ids
ORDER BY rum_data.hostname, rum_data.date
