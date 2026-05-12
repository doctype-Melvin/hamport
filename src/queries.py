FLIGHTS_WEATHER = """
        SELECT
            *
        FROM analytics.mart_weather_impact
        ORDER BY 1 desc
    """

ALL_FLIGHTS = """
        SELECT
            *
        FROM analytics.mart_flights
    """

FLIGHTS_VOLUME = """
        SELECT
            date,
            count(*) as flight_count
        FROM analytics.mart_flights
        WHERE date > '2026-04-30'
        GROUP BY 1
        ORDER BY 1
    """

WEATHER_HISTORY = """
        SELECT
        *
        FROM analytics.fct_weather_history
        ORDER BY 2 desc
        LIMIT 7
    """

AIRPORT_STATS = """
        SELECT
        *
        FROM analytics.mart_airport_hourly_stats
    """

AIRLINES_STATS = """
        SELECT
        *
        FROM analytics.mart_airlines_stats
    """

ORIGIN_DELAYS = """
        SELECT
        *
        FROM analytics.mart_airport_delays
    """

