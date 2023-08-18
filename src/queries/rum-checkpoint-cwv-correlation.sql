--- description: Using Helix RUM data, get a report of click rate by LCP Ntile, including a correlation coefficient.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- url: -
--- interval: 30
--- offset: 0
--- startdate: 2020-01-01
--- enddate: 2021-01-01
--- timezone: UTC
--- conversioncheckpoint: click
--- ntiles: 10
--- targets: https://, http://
--- domainkey: secret

WITH alldata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    lcp
  FROM
    `helix-225321.helix_rum.EVENTS_V3`(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      "UTC",
      "all",
      @domainkey
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
    NTILE(CAST(@ntiles AS INT64))
      OVER (ORDER BY ANY_VALUE(lcp)) AS lcp_percentile,
    COUNT(DISTINCT linkclickevents.target) AS linkclicks
  FROM linkclickevents FULL JOIN allids ON linkclickevents.id = allids.id
  INNER JOIN alllcps ON (alllcps.id = allids.id)
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
),

good_correlation AS (
  SELECT CORR(lcp, click_rate) AS good_correlation
  FROM clickrates
  WHERE lcp <= 2500 AND lcp_percentile > 1 # ignore the first percentile (outliers) and all values that are not good lcp
),

best_rates AS (
  SELECT
    lcp AS best_lcp,
    max_rate
  FROM (
    SELECT
      lcp,
      click_rate,
      MAX(click_rate) OVER () AS max_rate
    FROM clickrates
    ORDER BY lcp ASC
  )
  WHERE click_rate = max_rate
),

boost_potential AS (
  SELECT
    clickrates.lcp,
    clickrates.click_rate,
    best_rates.best_lcp,
    best_rates.max_rate,
    clickrates.lcp - best_rates.best_lcp AS lcp_diff,
    (best_rates.max_rate - clickrates.click_rate)
    / clickrates.click_rate AS click_rate_boost
  FROM clickrates
  FULL JOIN best_rates ON (true)
  WHERE clickrates.lcp - best_rates.best_lcp < 1000
  ORDER BY clickrates.lcp DESC
  LIMIT 1
)

SELECT
  clickrates.lcp_percentile,
  clickrates.lcp,
  clickrates.click_rate,
  correlation.correlation,
  good_correlation.good_correlation,
  boost_potential.click_rate_boost,
  boost_potential.lcp_diff
FROM clickrates
FULL JOIN correlation ON (true)
FULL JOIN good_correlation ON (true)
FULL JOIN boost_potential ON (true)
ORDER BY clickrates.lcp_percentile
