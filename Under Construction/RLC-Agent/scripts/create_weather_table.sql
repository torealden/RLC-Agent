-- Create weather_history table for agricultural weather data
-- Run with: psql -h localhost -U postgres -d rlc_commodities -f create_weather_table.sql

CREATE TABLE IF NOT EXISTS weather_history (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    country VARCHAR(5) NOT NULL,
    region VARCHAR(50) NOT NULL,
    lat DECIMAL(8,5) NOT NULL,
    lon DECIMAL(8,5) NOT NULL,
    date DATE NOT NULL,
    temp_max_f DECIMAL(5,1),
    temp_min_f DECIMAL(5,1),
    temp_mean_f DECIMAL(5,1),
    precipitation_mm DECIMAL(6,2),
    precipitation_hours DECIMAL(4,1),
    wind_speed_max_mph DECIMAL(5,1),
    wind_gusts_max_mph DECIMAL(5,1),
    soil_moisture_0_7cm DECIMAL(5,3),
    soil_temp_0_7cm_f DECIMAL(5,1),
    et0_mm DECIMAL(5,2),
    weather_code INTEGER,
    commodities TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, date)
);

CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_history(location_id);
CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_history(date);
CREATE INDEX IF NOT EXISTS idx_weather_region ON weather_history(region);
CREATE INDEX IF NOT EXISTS idx_weather_country ON weather_history(country);

COMMENT ON TABLE weather_history IS 'Historical daily weather data for key agricultural locations';
COMMENT ON COLUMN weather_history.et0_mm IS 'Reference evapotranspiration in mm';
COMMENT ON COLUMN weather_history.soil_moisture_0_7cm IS 'Soil moisture at 0-7cm depth (m³/m³)';
