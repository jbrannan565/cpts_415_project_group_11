import seaborn as sns
import geopandas

class GraphClass:
     """Plots the graphs"""
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

    def plot_k_cities_with_the_most_incoming_airlines(k=10):
        global querier
        # get data
        cities_with_airline_count = querier.get_top_k_cities_with_most_incoming_airlines(k=k).toPandas()

        # plot
        sns.set_theme(style="whitegrid")
        ax = sns.barplot(x='number', y='city', data=cities_with_airline_count)
        ax.set_xlabel("Number of Incoming Airlines")
        ax.set_ylabel("City")
        ax.set_title(f"{k} Cities with the Most Incoming Airlines")


    def plot_k_cosest_airports_to_city(city="Pullman", k=10, markersize=2):
        global querier

        cities = querier.get_k_closest_airport_to_city(city=city, k=k).toPandas()

        points = geopandas.points_from_xy(cities['airport_long'], cities['airport_lat'], crs="EPSG:4326")
        data = {'name': cities['Airport_Name'],'geometry': points}
        gdf = geopandas.GeoDataFrame(data)
        world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        base = world.boundary.plot(figsize=(20,60));
        base.set_title(f"Airports nearest {city}")
        gdf.plot(ax=base, marker='o', color='red')
