CREATE TABLE pageviews_counts(
    pagename VARCHAR(50) NOT NULL,
    pageviewcount INT NOT NULL, 
    datetime TIMESTAMP NOT NULL 
); 

SELECT x.pagename, x.hr AS "hour", x.average AS "average_pageview"
FROM (
    SELECT 
    pagename, 
    date_part("hour", datetime) AS hr, 
    AVG(pageviewcount) AS averageviews,
    ROW_NUMBER() OVER (PARTITION BY pagename ORDER BY AVG(pageviewcount) DESC) 
    FROM pageviews_counts
    GROUP BY pagename, hr

) AS x 
WHERE row_number = 1; 