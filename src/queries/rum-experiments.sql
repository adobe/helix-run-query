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
  SELECT * FROM helix_rum.CLUSTER_CHECKPOINTS('localhost:3000/drafts/uncled', -1, -7, '2022-02-01', '2022-05-28', 'GMT', 'all', '-')
),
experiment_checkpoints AS(
SELECT 
  source, 
  target, 
  id,
  ANY_VALUE(pageviews) AS pageviews, 
FROM all_checkpoints
WHERE checkpoint = 'experiment'
GROUP BY
  source,
  target,
  id
),
converted_checkpoints AS (
SELECT experiment_checkpoints.source, experiment_checkpoints.target, all_checkpoints.id, all_checkpoints.pageviews
FROM experiment_checkpoints JOIN all_checkpoints
  ON experiment_checkpoints.id = all_checkpoints.id
  WHERE all_checkpoints.checkpoint = 'click'
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
  SUM(pageviews) AS experimentations
FROM experiment_checkpoints
GROUP BY
  source,
  target
),
conversion_rates AS (
SELECT 
  experimentations_summary.source AS experiment,
  experimentations_summary.target AS variant,
  experimentation_events,
  conversion_events,
  experimentations,
  conversions,
  conversions / experimentations AS conversion_rate
FROM experimentations_summary FULL JOIN conversions_summary 
  ON experimentations_summary.source = conversions_summary.source 
  AND experimentations_summary.target = conversions_summary.target
)

-- SET
--   pooled_standard_error = SQRT( pooled_sample_proportion * ( 1 - pooled_sample_proportion ) * ( 1/sessions_variant_1 + 1/sessions_variant_2 ) );
-- SET
--   test = (conversions_variant_1/sessions_variant_1 - conversions_variant_2/sessions_variant_2) / pooled_standard_error;
-- SET
--   p_value = cdf((-1) * abs(test));

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
  (l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events) AS pooled_sample_proportion,
  SQRT(((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1 - ((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1/l.experimentations + 1/r.experimentations ))) AS pooled_standard_error,
  (l.conversion_rate - r.conversion_rate) / SQRT(((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1 - ((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1/l.experimentations + 1/r.experimentations ))) AS test,
  CDF((-1) * abs((l.conversion_rate - r.conversion_rate) / SQRT(((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1 - ((l.conversion_events + r.conversion_events) / (l.experimentation_events + r.experimentation_events)) * ( 1/l.experimentations + 1/r.experimentations ))))) AS p_value
 FROM conversion_rates AS l JOIN conversion_rates AS r ON
  l.experiment = r.experiment AND
  l.variant != r.variant
WHERE r.variant = 'control' AND l.variant != 'control'
LIMIT 100