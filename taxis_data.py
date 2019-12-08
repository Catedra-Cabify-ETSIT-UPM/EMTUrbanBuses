import pandas as pd
import geopandas as gpd

#If we only want one month
yellow_taxis = pd.read_csv('../NYCYellowTaxiData/2018/yellow_tripdata_2018-02.csv')

#We reduce sample size to improve debugging speed
yellow_taxis = yellow_taxis.sample(n=10000)

from datetime import datetime
def add_travel_time(row) :
    time = (datetime.strptime(row["finish_time"], '%Y-%m-%d %H:%M:%S') - datetime.strptime(row["start_time"], '%Y-%m-%d %H:%M:%S')).total_seconds()
    return round(time/60,3)

yellow_taxis = yellow_taxis.rename(columns={'tpep_pickup_datetime':'start_time','tpep_dropoff_datetime':'finish_time'})
yellow_taxis['duration'] = yellow_taxis.apply(add_travel_time, axis=1)

print(yellow_taxis.head())

# Read location ID regions
taxi_zones = gpd.read_file("../taxi_zones/taxi_zones.shp")
taxi_zones = taxi_zones.loc[:,['location_i','zone', 'geometry']].rename(columns={"location_i": "LocationID"}).dissolve(by='LocationID').reset_index()

print(taxi_zones.head())
