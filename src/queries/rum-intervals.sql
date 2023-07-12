--- description: For each of the specified intervals, report conversion rate, average LCP, FID, CLS, INP, standard error, and if there is a statistically significant difference between the latest interval and the current interval
--- url: -
--- startdate: 2021-06-22
--- enddate: 2021-06-22
--- timezone: UTC
--- conversioncheckpoint: click
--- targets: https://, http://
--- intervals: 2023-06-22,#target,2023-05-01,#mayday
--- domainkey: secret

CREATE TEMPORARY FUNCTION
CDF(nto FLOAT64)
RETURNS FLOAT64
LANGUAGE js AS """
{
    var mean = 0.0;
    var sigma = 1.0;
    var z = (nto-mean)/Math.sqrt(2*sigma*sigma);
    var t = 1/(1+0.3275911*Math.abs(z));
    var a1 =  0.254829592;
    var a2 = -0.284496736;
    var a3 =  1.421413741;
    var a4 = -1.453152027;
    var a5 =  1.061405429;
    var erf = 1-(((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t*Math.exp(-z*z);
    var sign = 1;
    if(z < 0)
    {
        sign = -1;
    }
    return (1/2)*(1+sign*erf);
}
""";

WITH prefixes AS (
  SELECT CONCAT(TRIM(prefix), "%") AS prefix
  FROM
    UNNEST(
      SPLIT(@targets, ",")
    ) AS prefix
),

intervals_input AS (
  SELECT prefix AS step
  FROM
    UNNEST(
      SPLIT(@intervals, ",")
    ) AS prefix
),

numbered_intervals AS (
  SELECT
    row_num,
    step AS current_val,
    LEAD(step) OVER (ORDER BY row_num ASC) AS next_val
  FROM (
    SELECT
      step,
      ROW_NUMBER() OVER () AS row_num
    FROM intervals_input
  )
),

named_intervals AS (
  SELECT
    interval_name,
    interval_start,
    COALESCE(
      LEAD(interval_start) OVER (ORDER BY interval_start), CURRENT_TIMESTAMP()
    ) AS interval_end
  FROM (
    SELECT
      next_val AS interval_name,
      TIMESTAMP(current_val) AS interval_start
    FROM numbered_intervals
    WHERE MOD(row_num, 2) = 1
  )
  ORDER BY interval_start DESC
),

alldata AS (
  SELECT
    id,
    checkpoint,
    source,
    target,
    lcp,
    time
  FROM
    `helix-225321.helix_rum.EVENTS_V3`(
      @url,
      -1, # not used
      -1, # not used
      @startdate,
      @enddate,
      @timezone,
      "all",
      @domainkey
    )
  WHERE weight > 0
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
  SELECT
    alldata.id,
    ANY_VALUE(named_intervals.interval_name) AS interval_name,
    ANY_VALUE(named_intervals.interval_start) AS interval_start,
    ANY_VALUE(named_intervals.interval_end) AS interval_end
  FROM alldata INNER JOIN named_intervals
    ON (
      named_intervals.interval_start <= alldata.time
      AND alldata.time < named_intervals.interval_end
    )
  GROUP BY alldata.id
),

events AS (
  SELECT
    allids.id,
    ANY_VALUE(allids.interval_name) AS interval_name,
    ANY_VALUE(allids.interval_start) AS interval_start,
    ANY_VALUE(allids.interval_end) AS interval_end,
    ANY_VALUE(alllcps.lcp) AS lcp,
    COUNT(DISTINCT linkclickevents.target) AS linkclicks
  FROM linkclickevents FULL JOIN allids ON linkclickevents.id = allids.id
  INNER JOIN alllcps ON (alllcps.id = allids.id)
  GROUP BY allids.id
  ORDER BY lcp DESC
),

intervals AS (
  SELECT
    interval_name,
    ANY_VALUE(interval_start) AS interval_start,
    ANY_VALUE(interval_end) AS interval_end,
    COUNT(DISTINCT id) AS events,
    SUM(linkclicks) AS conversions,
    AVG(lcp) AS lcp,
    STDDEV(lcp) AS lcp_stddev,
    SUM(linkclicks) / COUNT(DISTINCT id) AS conversion_rate,
    STDDEV(lcp) / SQRT(COUNT(DISTINCT id)) AS lcp_standard_error
  FROM events
  GROUP BY interval_name
),

last_interval AS (
  SELECT
    interval_name,
    interval_start,
    interval_end,
    events,
    conversions,
    lcp,
    lcp_stddev,
    conversion_rate,
    lcp_standard_error
  FROM intervals
  ORDER BY interval_start DESC
  LIMIT 1
),

all_results AS (
  SELECT
    l.interval_name,
    l.interval_start,
    l.interval_end,
    l.events,
    l.conversions,
    l.lcp,
    l.lcp_stddev,
    l.conversion_rate,
    l.lcp_standard_error,
    # lcp_p_value is the probability that the difference in LCP between the
    # current interval and the last interval is due to chance

  FROM intervals AS l INNER JOIN last_interval AS r
    ON (l.interval_name IS NOT NULL)
)

SELECT * FROM all_results

# Work in progress, steal from rum-checkpoint-cwv-correlation
