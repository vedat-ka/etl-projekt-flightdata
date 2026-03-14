-- Basic row counts
SELECT COUNT(*) AS airline_count FROM airlines;
SELECT COUNT(*) AS flight_count FROM flights;

-- Flights per destination airport
SELECT destination_airport_iata, COUNT(*) AS flights
FROM flights
GROUP BY destination_airport_iata
ORDER BY destination_airport_iata;

-- Top airlines by number of flights
SELECT a.name AS airline_name, COUNT(*) AS flights
FROM flights f
JOIN airlines a ON a.airline_id = f.airline_id
GROUP BY a.airline_id, a.name
ORDER BY flights DESC, airline_name ASC
LIMIT 10;

-- Top origin countries
SELECT origin_country, COUNT(*) AS flights
FROM flights
GROUP BY origin_country
ORDER BY flights DESC, origin_country ASC
LIMIT 10;

-- Check for duplicate business keys
SELECT source_record_key, COUNT(*) AS duplicates
FROM flights
GROUP BY source_record_key
HAVING COUNT(*) > 1;

-- Inspect a joined sample
SELECT
    f.flight_id,
    f.destination_airport_iata,
    f.origin_country,
    f.flight_number,
    a.name AS airline_name,
    f.arrival_time,
    f.arrival_year,
    f.arrival_month,
    f.arrival_day,
    f.arrival_hour,
    f.arrival_minute,
    f.arrival_second,
    f.status
FROM flights f
JOIN airlines a ON a.airline_id = f.airline_id
ORDER BY f.destination_airport_iata, f.arrival_time, f.flight_number
LIMIT 20;

-- Foreign key integrity check
PRAGMA foreign_key_check;