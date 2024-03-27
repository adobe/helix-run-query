  --- description: Add lhs data to bigquery
  --- Access-Control-Allow-Origin: *
  --- timezone: UTC
  --- url: -
  --- domainkey: secret
  --- perf_score: -1
  --- acc_score: -1
  --- bp_score: -1
  --- seo_score: -1
  --- perf_tti_score: -1
  --- perf_speed_idx: -1
  --- seo_crawl_score: -1
  --- seo_crawl_anchors_score: -1
  --- net_servr_time: -1
  --- net_nl: -1
  --- net_mainthread_work_score: -1
  --- net_total_blocking_score: -1
  --- net_img_optimization_score: -1
  --- third_party_score: -1
  --- device_type: -
  --- time: -
IF EXISTS (
  SELECT
    hostname_prefix,
    key_bytes,
    revoke_date
  FROM
    `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL
      OR revoke_date > CURRENT_DATE(@timezone))
    AND (hostname_prefix = ""
      OR hostname_prefix = @url)
    AND readonly = FALSE ) THEN
CALL
  helix_external_data.ADD_LHS_DATA( @url,
    @perf_score,
    @acc_score,
    @bp_score,
    @seo_score,
    @perf_tti_score,
    @perf_speed_idx,
    @seo_crawl_score,
    @seo_crawl_anchors_score,
    @net_servr_time,
    @net_nl,
    @net_mainthread_work_score,
    @net_total_blocking_score,
    @net_img_optimization_score,
    @third_party_score,
    @device_type,
    @time,
    @audit_ref );
END IF
  ;
SELECT
  "Lighthouse Data Successfully Uploaded" AS status,
  url,
  COUNT(*) AS number_of_row_instances
FROM
  `helix_external_data.lhs_spacecat`
WHERE
  url = @url
  AND time = @time
  AND audit_ref = @audit_ref
  AND device_type = @device_type
GROUP BY
  status,
  url