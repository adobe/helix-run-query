--- description: list for demo purposes
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- offset: 0
SELECT
  SPLIT(hlx_url, '--') [OFFSET(1)] AS repo,
  SPLIT(SPLIT(hlx_url, '--') [OFFSET(2)], '.') [OFFSET(0)] AS org
FROM `helix-225321.test_dataset.customers`
