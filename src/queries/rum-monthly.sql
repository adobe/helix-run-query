WITH domains_and_dates AS (
    SELECT
        id,
        REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1) AS url,
        MAX(time) AS date,
        MAX(CAST(weight AS INT64)) AS weight
    FROM `helix-225321.helix_rum.cluster`
    GROUP BY id
),

domains_and_months AS (
    SELECT
        weight,
        CASE url
            WHEN "www.adobe.com" THEN "Express"
            WHEN "pages.adobe.com" THEN "Pages"
            WHEN "blog.adobe.com" THEN "Blog"
            ELSE "Other"
        END AS url,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(YEAR FROM date) AS year
    FROM domains_and_dates
    WHERE url IS NOT NULL AND url NOT LIKE "%--%"
)

SELECT
    year,
    month,
    url,
    ROUND(SUM(weight) / 100000) / 10 AS pageviews
FROM domains_and_months
GROUP BY month, year, url
ORDER BY year DESC, month DESC
