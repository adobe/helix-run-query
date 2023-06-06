--- description: Get daily reported 404s for a site.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- url: 
--- granularity: 1
--- timezone: UTC
--- domainkey: secret
WITH validkeys AS (
    SELECT *
    FROM `helix-225321.helix_reporting.domain_keys`
    WHERE key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE('UTC'))
)
SELECT *
FROM `helix-225321.mrosier_test.cashub_404`
JOIN validkeys ON REGEXP_REPLACE(public_site, 'www.', '') = hostname_prefix OR hostname_prefix = ''
order by owner_repo