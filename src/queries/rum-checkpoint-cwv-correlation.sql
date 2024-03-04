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
--- sources: -
--- targets: -
--- domainkey: secret

WITH alldata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    lcp
  FROM
    `helix-225321.helix_rum.EVENTS_V5`(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      "all",
      @domainkey
    )
),

all_checkpoints AS (
  SELECT * FROM
    helix_rum.CHECKPOINTS_V5(
      @url, # domain or URL
      CAST(@offset AS INT64), # offset in days from today
      CAST(@interval AS INT64), # interval in days to consider
      @startdate, # not used, start date
      @enddate, # not used, end date
      @timezone, # timezone
      "all", # device class
      @domainkey
    )
),

source_target_converted_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(all_checkpoints.pageviews) AS pageviews
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @conversioncheckpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@sources, ",")) AS prefix
      WHERE all_checkpoints.source LIKE CONCAT(TRIM(prefix), "%")
    )
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@targets, ",")) AS prefix
      WHERE all_checkpoints.target LIKE CONCAT(TRIM(prefix), "%")
    )
  GROUP BY all_checkpoints.id
),

source_converted_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @conversioncheckpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@sources, ",")) AS prefix
      WHERE all_checkpoints.source LIKE CONCAT(TRIM(prefix), "%")
    )
  GROUP BY all_checkpoints.id
),

target_converted_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews
  FROM all_checkpoints
  WHERE
    all_checkpoints.checkpoint = @conversioncheckpoint
    AND EXISTS (
      SELECT 1
      FROM
        UNNEST(SPLIT(@targets, ",")) AS prefix
      WHERE all_checkpoints.target LIKE CONCAT(TRIM(prefix), "%")
    )
  GROUP BY all_checkpoints.id
),

loose_converted_checkpoints AS (
  SELECT
    all_checkpoints.id AS id,
    ANY_VALUE(source) AS source,
    ANY_VALUE(target) AS target,
    ANY_VALUE(pageviews) AS pageviews
  FROM all_checkpoints
  WHERE all_checkpoints.checkpoint = @conversioncheckpoint
  GROUP BY all_checkpoints.id
),

converted_checkpoints AS (
  SELECT * FROM loose_converted_checkpoints
  WHERE @sources = "-" AND @targets = "-"
  UNION ALL
  SELECT * FROM source_target_converted_checkpoints
  WHERE @sources != "-" AND @targets != "-"
  UNION ALL
  SELECT * FROM source_converted_checkpoints
  WHERE @sources != "-" AND @targets = "-"
  UNION ALL
  SELECT * FROM target_converted_checkpoints
  WHERE @sources = "-" AND @targets != "-"
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
    COUNT(DISTINCT converted_checkpoints.target) AS linkclicks
  FROM converted_checkpoints FULL JOIN allids
    ON converted_checkpoints.id = allids.id
  INNER JOIN alllcps ON (allids.id = alllcps.id)
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
  FULL JOIN best_rates ON (TRUE)
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
FULL JOIN correlation ON (TRUE)
FULL JOIN good_correlation ON (TRUE)
FULL JOIN boost_potential ON (TRUE)
ORDER BY clickrates.lcp_percentile
--- lcp_percentile: the nth ntiles of LCP values (number of values is based on the ntile parameter)
--- lcp: the mean LCP value for the nth ntile
--- click_rate: the click rate for the nth ntile
--- correlation: the correlation coefficient between LCP and click rate (negative means that a higher LCP is correlated with a lower click rate)
--- good_correlation: the correlation coefficient between LCP and click rate, but only for LCP values that are less than 2500ms (this is where the correlation is strongest)
--- click_rate_boost: the percentage increase in click rate if the LCP was by one second (1000ms) faster
--- lcp_diff: the difference between the LCP and the best LCP (the LCP that has the highest click rate)
