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