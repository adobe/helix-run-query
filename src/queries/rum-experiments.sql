--- description: Using Helix RUM data, get a report of conversion rates of experiment variants compared to control, including p value.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- domain: -
--- interval: 30
--- offset: 0
--- experiment: -
--- conversioncheckpoint: click

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

WITH
all_checkpoints AS (
  SELECT * FROM helix_rum.CLUSTER_CHECKPOINTS(
    @domain, # domain or URL
    CAST(@offset AS INT64), # offset in days from today
    CAST(@interval AS INT64), # interval in days to consider
    '2022-02-01', # not used, start date
    '2022-05-28', # not used, end date
    'GMT', # timezone
    'all', # device class
    '-' # not used, generation
  )
),

experiment_checkpoints AS (
  SELECT
    source,
    target,
    id,
    APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
    APPROX_QUANTILES(time, 100)[OFFSET(95)] AS t95,
    APPROX_QUANTILES(time, 100)[OFFSET(5)] AS t5,
    ANY_VALUE(pageviews) AS pageviews
  FROM all_checkpoints
  WHERE checkpoint = 'experiment'
    # filter by experiment or show all
    AND (source = @experiment OR @experiment = '-')
  GROUP BY
    source,
    target,
    id
),

converted_checkpoints AS (
  SELECT
    experiment_checkpoints.source,
    experiment_checkpoints.target,
    all_checkpoints.id,
    all_checkpoints.pageviews
  FROM experiment_checkpoints INNER JOIN all_checkpoints
    ON experiment_checkpoints.id = all_checkpoints.id
  # note: at some point we may need further filters here
  WHERE all_checkpoints.checkpoint = @conversioncheckpoint
),

conversions_summary AS (
  SELECT
    source,
    target,
    COUNT(DISTINCT id) AS conversion_events,
    SUM(pageviews) AS conversions
  FROM converted_checkpoints
  GROUP BY
    source,
    target
),

experimentations_summary AS (
  SELECT
    source,
    target,
    COUNT(DISTINCT id) AS experimentation_events,
    SUM(pageviews) AS experimentations,
    ANY_VALUE(topurl) AS topurl,
    MAX(t95) AS t95,
    MIN(t5) AS t5
  FROM experiment_checkpoints
  GROUP BY
    source,
    target
),

conversion_rates AS (
  SELECT
    experimentations_summary.source AS experiment,
    experimentations_summary.target AS variant,
    experimentations_summary.experimentation_events,
    conversions_summary.conversion_events,
    experimentations_summary.experimentations,
    conversions_summary.conversions,
    experimentations_summary.topurl AS topurl,
    experimentations_summary.t95 AS t95,
    experimentations_summary.t5 AS t5,
    conversions_summary.conversions / experimentations_summary.experimentations
    AS conversion_rate
  FROM experimentations_summary FULL JOIN conversions_summary
    ON experimentations_summary.source = conversions_summary.source
      AND experimentations_summary.target = conversions_summary.target
)

SELECT
  l.experiment,
  l.variant,
  l.experimentation_events AS variant_experimentation_events,
  r.experimentation_events AS control_experimentation_events,
  l.conversion_events AS variant_conversion_events,
  r.conversion_events AS control_conversion_events,
  l.experimentations AS variant_experimentations,
  r.experimentations AS control_experimentations,
  l.conversions AS variant_conversions,
  r.conversions AS control_conversions,
  l.conversion_rate AS variant_conversion_rate,
  r.conversion_rate AS control_conversion_rate,
  l.topurl AS topurl,
  CAST(l.t95 AS STRING) AS time95,
  CAST(l.t5 AS STRING) AS time5,
  # Math!
  (
    l.conversion_events + r.conversion_events
  ) / (
    l.experimentation_events + r.experimentation_events
  ) AS pooled_sample_proportion,
  SQRT(
    (
      (
        l.conversion_events + r.conversion_events
      ) / (l.experimentation_events + r.experimentation_events)
    ) * (
      1 - (
        (
          l.conversion_events + r.conversion_events
        ) / (l.experimentation_events + r.experimentation_events)
      ) * ( 1 / l.experimentations + 1 / r.experimentations )
    )
  ) AS pooled_standard_error,
  (
    l.conversion_rate - r.conversion_rate
  ) / SQRT(
    (
      (
        l.conversion_events + r.conversion_events
      ) / (l.experimentation_events + r.experimentation_events)
    ) * (
      1 - (
        (
          l.conversion_events + r.conversion_events
        ) / (l.experimentation_events + r.experimentation_events)
      ) * ( 1 / l.experimentations + 1 / r.experimentations )
    )
  ) AS test,
  CDF(
    (
      -1
    ) * ABS(
      (
        l.conversion_rate - r.conversion_rate
      ) / SQRT(
        (
          (
            l.conversion_events + r.conversion_events
          ) / (l.experimentation_events + r.experimentation_events)
        ) * (
          1 - (
            (
              l.conversion_events + r.conversion_events
            ) / (l.experimentation_events + r.experimentation_events)
          ) * ( 1 / l.experimentations + 1 / r.experimentations )
        )
      )
    )
  ) AS p_value
FROM conversion_rates AS l INNER JOIN conversion_rates AS r ON
    l.experiment = r.experiment
    AND l.variant != r.variant
WHERE r.variant = 'control' AND l.variant != 'control'
LIMIT 100
