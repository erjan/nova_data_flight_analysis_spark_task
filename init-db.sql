-- Create schema for flights data
CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    date DATE,
    day_of_week INTEGER,
    airline VARCHAR(10),
    flight_number INTEGER,
    tail_number VARCHAR(20),
    origin_airport VARCHAR(10),
    destination_airport VARCHAR(10),
    departure_delay FLOAT,
    distance FLOAT,
    arrival_delay FLOAT,
    diverted INTEGER,
    cancelled INTEGER,
    cancellation_reason VARCHAR(1),
    air_system_delay FLOAT,
    security_delay FLOAT,
    airline_delay FLOAT,
    late_aircraft_delay FLOAT,
    weather_delay FLOAT,
    departure_hour INTEGER,
    arrival_hour INTEGER,
    is_long_haul BOOLEAN,
    day_part VARCHAR(20)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_airline ON flights(airline);
CREATE INDEX IF NOT EXISTS idx_origin_airport ON flights(origin_airport);
CREATE INDEX IF NOT EXISTS idx_destination_airport ON flights(destination_airport);
CREATE INDEX IF NOT EXISTS idx_date ON flights(date);
CREATE INDEX IF NOT EXISTS idx_day_part ON flights(day_part);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE flights TO datauser;
GRANT USAGE, SELECT ON SEQUENCE flights_id_seq TO datauser;
