-- Partition Key: country
-- Clustering Keys: state, city, gadm_level
-- Reasoning: Efficient searches based on a known country → state → city hierarchy
CREATE TABLE IF NOT EXISTS gadm_data (
    country TEXT,
    state TEXT,
    city TEXT,
    gadm_level TEXT,
    wkt_geometry TEXT,
    PRIMARY KEY (country, state, city, gadm_level)
);

-- Partition Key: (country, state, city)
-- Clustering Key: postal_code
-- Reasoning: Optimized for searches where we know the location hierarchy.
CREATE TABLE IF NOT EXISTS wof_data (
    postal_code TEXT,
    country TEXT,
    state TEXT,
    city TEXT,
    lat DOUBLE,
    lon DOUBLE,
    wkt_geometry TEXT,
    PRIMARY KEY ((country, state, city), postal_code)
);
