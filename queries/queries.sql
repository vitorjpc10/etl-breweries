-- Query to view breweries table
SELECT * FROM breweries;

-- Query for aggregated view with the quantity of breweries per type and location.
SELECT
    city,
    state,
    brewery_type,
    COUNT(*) AS brewery_count
FROM
    breweries
GROUP BY
    city,
    state,
    brewery_type;