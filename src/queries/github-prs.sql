--- description: Get Github Development Data for a Site
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- url: 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
SELECT *
FROM `helix-225321.mrosier_test.cashub_franklin_prs`
order by owner_repo