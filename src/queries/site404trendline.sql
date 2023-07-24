--- description: Get daily count of reported 404s.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=86400
--- interval: 30
--- offset: 0
--- url: ''
--- ownerrepo: '' 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
WITH validkeys AS (
  SELECT hostname_prefix
  FROM `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE('UTC'))
)

SELECT
  c404.public_site,
  c404.owner_repo,
  sum(c404.req_count),
  c404.date
FROM `helix-225321.mrosier_test.cashub_404` AS c404
INNER JOIN
  validkeys
  ON
    REGEXP_REPLACE(c404.public_site, 'www.', '') = validkeys.hostname_prefix
    OR validkeys.hostname_prefix = ''
where c404.public_site = @url or c404.owner_repo = @ownerrepo
group by c404.public_site, c404.owner_repo, c404.date 
ORDER BY c404.owner_repo
