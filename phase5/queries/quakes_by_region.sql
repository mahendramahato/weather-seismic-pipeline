-- Earthquakes per region. USGS "place" looks like "6 km WSW of Hermosa Beach, CA":
-- the text after the last comma is the region. Some places have no comma
-- ("Balleny Islands region"), so fall back to the whole place.
SELECT
    coalesce(nullif(trim(regexp_extract(place, ',([^,]*)$', 1)), ''), place) AS region,
    COUNT(*)                   AS quakes,
    ROUND(MAX(magnitude), 1)   AS biggest
FROM curated_seismic
GROUP BY 1
ORDER BY quakes DESC;
