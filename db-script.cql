CREATE KEYSPACE IF NOT EXISTS datasnakedataprepkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE datasnakedataprepkeyspace;
DROP TABLE gadm0_data;
DROP TABLE gadm1_data;
DROP TABLE gadm2_data;

-- Partition Key: country
-- Clustering Keys: state, city, gadm_level
-- Reasoning: Efficient searches based on a known country → state → city hierarchy
CREATE TABLE IF NOT EXISTS gadm0_data (
    country_code TEXT,
    country_full_name TEXT,
    gadm_level TEXT,
    PRIMARY KEY (country_code)
);

CREATE TABLE IF NOT EXISTS gadm1_data (
    country_code TEXT,
    state TEXT,
    shapeID TEXT,
    gadm_level TEXT,
    wkt_geometry_state TEXT,
    PRIMARY KEY (country_code, state)
);

CREATE TABLE IF NOT EXISTS gadm2_data (
    country_code TEXT,
    city TEXT,
    shapeID TEXT,
    gadm_level TEXT,    
    PRIMARY KEY (country_code, city)
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

DROP TABLE stocks;
CREATE TABLE IF NOT EXISTS stocks (
    stock_id UUID,
    symbol TEXT,
    price FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY (symbol, timestamp )
) WITH CLUSTERING ORDER BY ( timestamp DESC );