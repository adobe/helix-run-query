--- description: Get Github Development Data for a Site
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-control: max-age=300
--- interval: 30
--- offset: 0
--- url: 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
SELECT
  pr_url,
  repository,
  title,
  user,
  pr_id,
  pr_number,
  created_at,
  merged_at,
  owner_repo
FROM `helix-225321.mrosier_test.cashub_franklin_prs`
ORDER BY owner_repo
