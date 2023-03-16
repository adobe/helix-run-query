--- description: Using Helix RUM data, get a report of click rate by LCP Ntile, including a correlation coefficient.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- domain: -
--- interval: 30
--- offset: 0
--- startdate: 2020-01-01
--- enddate: 2021-01-01
--- timezone: UTC
--- conversioncheckpoint: click
--- ntiles: 10
--- targets: https://, http://

WITH alldata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    lcp
  FROM
    `HELIX-225321.HELIX_RUM.CLUSTER_EVENTS`(
      @domain, @offset, @interval, @startdate, @enddate, @timezone, "all", "-"
    )
),

prefixes AS (
  SELECT CONCAT(TRIM(prefix), "%") AS prefix
  FROM
    UNNEST(
      SPLIT(@targets, ",")
    ) AS prefix
),

linkclickevents AS (
  SELECT
    alldata.id,
    alldata.checkpoint,
    alldata.target
  FROM alldata INNER JOIN prefixes
    ON (alldata.target LIKE prefixes.prefix)
  WHERE alldata.checkpoint = @conversioncheckpoint
),

alllcps AS (
  SELECT
    id,
    ANY_VALUE(lcp) AS lcp
  FROM alldata
  WHERE lcp IS NOT NULL
  GROUP BY id
),

allids AS (
  SELECT id FROM alldata
  GROUP BY id
),

events AS (
  SELECT
    allids.id,
    ANY_VALUE(alllcps.lcp) AS lcp,
    NTILE(@ntiles) OVER(ORDER BY ANY_VALUE(lcp)) AS lcp_percentile,
    COUNT(DISTINCT linkclickevents.target) AS linkclicks
  FROM linkclickevents FULL JOIN allids ON linkclickevents.id = allids.id
  INNER JOIN alllcps ON (allcps.id = allids.id)
  GROUP BY allids.id
  ORDER BY lcp DESC
),

clickrates AS (
  SELECT
    lcp_percentile,
    AVG(lcp) AS lcp,
    AVG(linkclicks) AS click_rate
  FROM events
  GROUP BY lcp_percentile
  ORDER BY lcp_percentile ASC
),

correlation AS (
  SELECT CORR(lcp, click_rate) AS correlation FROM clickrates
)


SELECT
  clickrates.lcp_percentile,
  clickrates.lcp,
  clickrates.click_rate,
  correlation.correlation
FROM clickrates FULL JOIN correlation ON (true)
ORDER BY clickrates.lcp_percentile


# SELECT
#   target,
#   COUNT(DISTINCT id) AS ids
# FROM linkclickevents
# GROUP BY target
# ORDER BY ids DESC