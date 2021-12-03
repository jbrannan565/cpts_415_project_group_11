from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

class AirlineDataQuerier:
    """Run queries against an apache spark instance to get Airline info"""
    def __init__(self, master='local[*]', appname='Milestone3'):
        # Create spark session
        self.spark = SparkSession. \
            builder. \
            appName(appname). \
            master(master). \
            config("spark.serializer", KryoSerializer.getName). \
            config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
            config('spark.jars.packages',
                   'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,'
                   'org.datasyslab:geotools-wrapper:geotools-24.1'). \
            getOrCreate()

        SedonaRegistrator.registerAll(self.spark)
        
        self.load_tables()
        
    def load_tables(self):
        """
        Read in tables from data files.
        
        Tables:
        - Airports
        - Airlines
        - states
        - Cities
        - Routes
        """
        # read in airports
        airports = self.spark.read.option("delimiter", ",").option("header", "false").csv("clean_data/airports.csv") \
            .toDF("IDX", "Airport_ID","Name","City","Country","IATA","ICAO","Latitude","Longitude","Altitude","Timezone","DST","Tz_database_timezone","Type","Source")

        airports.createOrReplaceTempView("Airports")
        
        # read in airlines
        airlines = self.spark.read.option("delimiter", ",").option("header", "false").csv("clean_data/airlines.csv") \
            .toDF("IDX","Airline_ID","Name","Alias","IATA","ICAO","Callsign","Country","Active")

        airlines.createOrReplaceTempView("Airlines")
        
        # read in states
        boundary_each_state = self.spark.read.option("delimiter", "\t").option("header", "false").csv("clean_data/boundary-each-state.tsv") \
            .toDF("s_name","s_bound")
        boundary_each_state = boundary_each_state.selectExpr("s_name", "ST_GeomFromWKT(s_bound) as s_bound")
        boundary_each_state.createOrReplaceTempView("states")
        
        # read in cities
        cities = self.spark.read.option("delimiter", ",").option("header", "false").csv("clean_data/cities.csv") \
            .toDF("Name","Longitude","Latitude")

        cities.createOrReplaceTempView("Cities")
        
        # read in routes
        routes = self.spark.read.option("delimiter", ",").option("header", "false").csv("clean_data/routes.csv") \
            .toDF('IDX', 'Airline', 'Airline_ID', 'Source_airport',
               'Source_airport_ID', 'Destination_airport', 'Destination_airport_ID',
               'Codeshare', 'Stops', 'Equipment')

        routes.createOrReplaceTempView("Routes")
    
    def run_query(self, query):
        """Run a query against apache spark instance"""
        return self.spark.sql(query)
    
    def get_airports_in_country(self, country_name):
        """Get a list of Airport_IDs and Names of the airports in country `country_name`"""
        query = f"SELECT Airport_ID, Name FROM airports WHERE Country LIKE '%{country_name}%'"
        return self.run_query(query)
    
    def get_airlines_with_x_stops(self, stop_value):
        """Get a list of Airport_IDs and Names that have `stop_value` stops"""
        query = f"""
        SELECT DISTINCT a.Airline_ID, a.Name, r.stops
        FROM Airlines AS a, Routes AS r
        INNER JOIN Routes ON a.Airline_ID = r.Airline_ID
        WHERE r.Stops = {stop_value}"""
        return self.run_query(query)
    
    def get_airlines_with_code_share(self):
        """Get airlines that have codeshare"""
        query = f"""
        SELECT DISTINCT Airline
        FROM routes
        WHERE Codeshare = 'Y'
        """
        return self.run_query(query)
    
    def get_active_airlines_in_country(self, country_name):
        """Get the airlines that are active in country `country_name`"""
        query = f"""
        SELECT Airline_ID, Name
        FROM Airlines
        WHERE Airlines.Country LIKE '%{country_name}%' AND Airlines.active = 'Y'
        """
        return self.run_query(query)

    def get_active_airlines_in_United_States(self):
        """Get the airlines that are active in United States"""
        return self.get_active_airlines_in_country("United States")
    
    def get_country_or_teritory_with_most_airports(self):
        """Get the country or teritory with the most airports"""
        query = f"""
        SELECT * FROM (
        SELECT COUNT(Airlines.Airline_ID) as number, Airlines.Country
        FROM Airlines
        GROUP BY Airlines.Country)
        ORDER BY number DESC
        LIMIT 1
        """
        return self.run_query(query)
    
    def get_top_k_cities_with_most_incoming_airlines(self, k):
        """Get the top `k` cities with the most incoming airlines"""
        query = f"""
        SELECT count(Routes.airline) as number, Airports.city
        FROM Routes JOIN Airports ON Routes.Destination_airport_ID = Airports.Airport_ID
        GROUP BY Airports.city
        ORDER BY number DESC
        LIMIT {k}
        """
        return self.run_query(query)
    
    def get_closest_airport_to_city(self, city):
        """
        Find the closest airport to `city` and get the Airport_Name, 
        Airport_ID and distance between the city and the airport
        """
        query = f"""
        SELECT SQRT(POW(a_lat - c_lat, 2) + POW(a_long - c_long, 2)) as dist, Airport_Name, Airport_ID
        FROM
        (SELECT a.Latitude as a_lat, a.Longitude as a_long, a.Name as Airport_Name, a.Airport_ID, c.Latitude as c_lat, c.Longitude as c_long
        FROM
        (SELECT Airports.Latitude, Airports.Longitude, Airports.Name, Airports.Airport_ID
        FROM Airports
        WHERE Latitude NOT LIKE 'Latitude' AND Longitude NOT LIKE 'Longitude'
        ) as a 
        INNER JOIN
        (SELECT Cities.Latitude, Cities.Longitude 
        FROM Cities  WHERE Name='{city}' LIMIT 1) as c)
        ORDER BY dist ASC
        LIMIT 1
        """

        return self.run_query(query)
    
    def get_k_closest_airport_to_city(self, city, k):
        """
        Find the k closest airport to `city` and get the Airport_Name, 
        Airport_ID and distance between the city and the airport
        """
        query = f"""
        SELECT ST_Distance(
                ST_GeomFromWKT(CONCAT('POINT(', c_long, ' ', c_lat, ')')),
                ST_GeomFromWKT(CONCAT('POINT(', a_lat, ' ', a_long, ')'))                
            ) as dist, 
        a_lat as airport_lat, a_long as airport_long, 
        c_lat as city_lat, c_long as city_long,
        Airport_Name, Airport_ID
        FROM
        (SELECT a.Latitude as a_lat, a.Longitude as a_long, a.Name as Airport_Name, a.Airport_ID, c.Latitude as c_lat, c.Longitude as c_long
        FROM
        (SELECT Airports.Latitude, Airports.Longitude, Airports.Name, Airports.Airport_ID
        FROM Airports
        WHERE Latitude NOT LIKE 'Latitude' AND Longitude NOT LIKE 'Longitude'
        ) as a 
        INNER JOIN
        (SELECT Cities.Latitude, Cities.Longitude 
        FROM Cities  WHERE Name='{city}' LIMIT 1) as c)
        ORDER BY dist ASC
        LIMIT {k}
        """

        return self.run_query(query)
    
    def get_airport_in_each_state(self):
        """Get the airport in each state, selecting the Airport_ID, Airport_name, and s_name (State name)"""
        query = f"""
        SELECT _contains, Airport_name, Airport_ID, s_name as state_name
        FROM
        (
            SELECT
            ST_Contains(s_bound, c_point) as _contains, Name as Airport_name, Airport_ID, s_name
            FROM
            (
                SELECT * FROM states
            ) AS s
            INNER JOIN
            (
                SELECT ST_GeomFromWKT(CONCAT('POINT(', Longitude, ' ', Latitude, ')')) as c_point, Name, Airport_ID
                FROM Airports 
                WHERE Longitude NOT LIKE 'Longitude' AND Latitude NOT LIKE 'Latitude'
            ) AS c
        )
        WHERE _contains=True
        ORDER BY state_name
        """

        return self.run_query(query)