CREATE OR REPLACE TEMP TABLE all_results
(
  is_admin BOOLEAN
);

INSERT INTO all_results
(is_admin)
VALUES
(false);

# hlx:metadata
SELECT 100 AS total_rows;

SELECT is_admin FROM all_results LIMIT 1;
