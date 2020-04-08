import pandas as pd
import json

import datetime
from datetime import timedelta

from sys import argv

from pandarallel import pandarallel
from joblib import Parallel, delayed
import multiprocessing
num_cores = multiprocessing.cpu_count()
pandarallel.initialize()

# WE LOAD THE STOPS AND LINES DATA
lines_shapes = pd.read_csv('../M6Data/lines_shapes.csv')
#Load line_stops_dict
with open('../M6Data/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)

#FUNCTIONS
def calculate_coords(df,stop_id,dist_to_stop) :
    '''
    Returns the calculated coordinates of the bus

    Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the calculated coords
        stop : str
        dist_to_stop : int
            Distance to the stop in meters
    '''
    line_sn = df.iloc[0].line_sn
    direction = str(df.iloc[0].direction)
    bus_distance = int(lines_collected_dict[line_sn][direction]['distances'][str(stop_id)]) - dist_to_stop
    nearest_row = find_nearest_row_by_dist(df,bus_distance)
    if nearest_row :
        return nearest_row.lon, nearest_row.lat
    else :
        return 0,0


def find_nearest_row_by_dist(df,dist_traveled) :
    """
    Returns the row nearest to the distance traveled passed in the dataframe

        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        dist_traveled : int
            Distance to origin of the line in meters
    """
    min_dist_error = 1000000.0
    df_reduced = df.loc[(df.dist_traveled>dist_traveled-100)&(df.dist_traveled<dist_traveled+100)]
    if df_reduced.shape[0]!=0:
        for row in df_reduced.itertuples() :
            error = abs(row.dist_traveled-dist_traveled)
            if  error < min_dist_error :
                min_dist_error = error
                nearest_row = row
    else :
        nearest_row = None
    return nearest_row


def process_bus_df(bus,df_stop,threshold,line_shape):
    '''
    Processes the rows adding day_trip, arrival time and calculated coordinates attributes
    '''
    df_bus = df_stop.loc[df_stop.bus == bus].sort_values(by='datetime').reset_index(drop=True)
    stop_id = df_bus.iloc[0].stop
    last_index = 0
    last_time = df_bus.iloc[0].datetime
    day_trip = 0
    trips,calc_lats,calc_lons = [],[],[]
    for i in range(df_bus.shape[0]) :
        #Calculate coordinates
        #calc_lon,calc_lat = calculate_coords(line_shape,stop_id,df_bus.iloc[i]['DistanceBus'])
        #calc_lons.append(calc_lon)
        #calc_lats.append(calc_lat)

        #Estimate arrival time for each slice
        if ((df_bus.iloc[i].datetime - last_time).total_seconds() > 600) | (i==df_bus.shape[0]-1) :
            #Trip dataframe
            df_trip = df_bus.iloc[last_index:i]

            if df_trip.shape[0] != 0 :
                #Trip number inside the day
                day_trip += 1

                #Get first row with estimateArrive < threshold seconds
                df_close = df_trip.loc[df_trip.estimateArrive<threshold]
                if df_close.shape[0] != 0 :
                    row = df_close.sort_values(by='datetime',ascending='True').iloc[0]
                else :
                    row = df_trip.loc[df_trip.estimateArrive==df_trip.estimateArrive.min()].iloc[0]

                #Assign arrival time and trip day
                df_trip = df_trip.assign(
                    day_trip=day_trip,
                    arrival_time=row.datetime + timedelta(seconds=int(row.estimateArrive)),
                    #calc_lat=calc_lats[last_index:i],
                    #calc_lon=calc_lons[last_index:i]
                )
                trips.append(df_trip)
                last_index = i
        #Update last time value
        last_time = df_bus.iloc[i].datetime
    return trips


def add_arrival_time_estim(df,threshold) :
    '''
    Returns the dataframe with a new column with the estimation of the time when the bus has arrived the stop
    that is giving the estimation, and another column with the trip index in the day. The estimation
    is based on the value of ''estimateArrive'' for the first row that is less than threshold
    seconds away from the stop.

    May take a long time for big dataframes

    Parameters
    ----------------------
    df : The dataframe where we wish to add the column

    '''
    #Get number of days lapsed
    days = (df.datetime.max()-df.datetime.min()).days + 1
    first_date = df.datetime.min()
    #List to add the trip dataframes
    trips = []
    for day in range(days) :
        df_day = df.loc[df.datetime.dt.date == (first_date+timedelta(days=day))]
        #For each line of the bus.
        lines = df_day.line.unique().tolist()
        for line in lines :
            df_line = df_day.loc[df_day.line == line]
            #For each destination in that line
            for dest in lines_collected_dict[line]['destinations'] :
                direction = '1' if dest == lines_collected_dict[line]['destinations'][1] else '2'
                line_shape = lines_shapes.loc[(lines_shapes.line_sn == line)&(lines_shapes.direction == int(direction))]
                df_dest = df_line.loc[df.destination == dest]
                #For each stop in that line and destination
                for stop in lines_collected_dict[line][direction]['stops'] :
                    df_stop = df_dest.loc[df_dest.stop == int(stop)]
                    #For each bus in that line destination and stop
                    buses = df_stop.bus.unique().tolist()
                    trips += sum((Parallel(n_jobs=num_cores)(delayed(process_bus_df)(bus,df_stop,threshold,line_shape) for bus in buses)), [])
    new_df = pd.concat(trips).sort_values(by='datetime',ascending='True')
    return new_df[['line','destination','stop','bus','day_trip','datetime','estimateArrive','DistanceBus','arrival_time','given_coords','lat','lon']]


def clean_data(df,preprocess) :
    '''
    Returns the dataframe without the rows that dont match the conditions specified

    Parameters
    ----------------
    df : DataFrame
        Dataframe to clean
    processed : bool
        Boolean that indicates if the data has been processed
    '''
    def check_conditions(row) :
        '''
        Checks if every row in the dataframe matchs the conditions

        Parameters
        ----------------
        row : DataFrame
            Dataframe to clean
        processed : bool
            Boolean that indicates if the data has been processed
        '''
        #Direction
        direction = '1' if row.destination == lines_collected_dict[row.line]['destinations'][1] else '2'

        #Line destination stop coherence condition
        line_dest_stop_cond = False
        if row.line in lines_collected_dict.keys() :
            if row.destination in lines_collected_dict[row.line]['destinations'] :
                if str(row.stop) in lines_collected_dict[row.line][direction]['stops'] :
                    line_dest_stop_cond = True

        # DistanceBus values lower than the line length or negative
        dist_cond = (row.DistanceBus >= 0) and \
                    (row.DistanceBus < int(lines_collected_dict[row.line][direction]['length']))

        # estimateArrive values lower than the time it takes to go through the line at an speed
        # of 2m/s, instantaneous speed lower than 120 km/h and positive values and time remaining lower than 2 hours
        eta_cond = (row.estimateArrive > 0) and \
                   (row.estimateArrive < (int(lines_collected_dict[row.line][direction]['length'])/2)) and \
                   ((3.6*row.DistanceBus/row.estimateArrive) < 120) & \
                   (row.estimateArrive < 7200)

        return line_dest_stop_cond and dist_cond and eta_cond

    #Check conditions in df
    mask = df.parallel_apply(check_conditions,axis=1)
    #Select rows that match the conditions
    df = df.loc[mask].reset_index(drop=True)
    #Return cleaned DataFrame
    return df



def main():
    #Read passed parameters
    preprocess,clean = False,False
    f = '../ProcessedData/buses_data_'
    if len(argv)>1:
        if ('p' in argv[1]) :
            preprocess = True
            f = f + 'p'
        if ('c' in argv[1]) :
            clean = True
            f = f + 'c'
        if not (clean or preprocess) :
            print('Arguments passed not valid; use -p for preprocess, -p for clean or -pc for both.\n')
            exit(0)
    else :
        print('Arguments passed not valid; use -p for preprocess, -p for clean or -pc for both.\n')
        exit(0)
    f = f + '.csv'

    # WE LOAD THE ARRIVAL TIMES DATA
    now = datetime.datetime.now()
    print('\n-------------------------------------------------------------------')
    print('Reading the original data... - {}\n'.format(now))
    buses_data = pd.read_csv('../buses_data.csv',
        dtype={
            'line': 'str',
            'destination': 'str',
            'stop': 'uint16',
            'bus': 'uint16',
            'given_coords': 'bool',
            'pos_in_burst':'uint16',
            'estimateArrive': 'int32',
            'DistanceBus': 'int32',
            'request_time': 'int32',
            'lat':'float32',
            'lon':'float32'
        }
    )[['line','destination','stop','bus','datetime','estimateArrive','DistanceBus','given_coords','lat','lon']]

    #Parse the dates
    buses_data['datetime'] = pd.to_datetime(buses_data['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
    print(buses_data.info())
    lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    #Preprocess data; adds day_trip, arrival_time and calculated coordinates attributes
    if preprocess :
        now = datetime.datetime.now()
        print('-------------------------------------------------------------------')
        print('Preprocessing the data... - {}\n'.format(now))
        buses_data = add_arrival_time_estim(buses_data,45)
        print(buses_data.info())
        lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
        print('\nFinished in {} seconds'.format(lapsed_seconds))
        print('-------------------------------------------------------------------\n\n')


    #Clean the data
    if clean :
        now = datetime.datetime.now()
        print('-------------------------------------------------------------------')
        print('Cleaning the data... - {}\n'.format(now))
        buses_data = clean_data(buses_data,preprocess)
        print(buses_data.info())
        lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
        print('\nFinished in {} seconds'.format(lapsed_seconds))
        print('-------------------------------------------------------------------\n\n')


    #Processed data info
    now = datetime.datetime.now()
    print('-------------------------------------------------------------------')
    print('Writting new data to {}... - {}'.format(f,now))
    #Write result to file
    buses_data.to_csv(f)
    lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    print('New data is ready!\n')



if __name__== "__main__":
    main()
