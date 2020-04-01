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

#Load line_stops_dict
with open('M6Data/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)
#Load times between stops data
times_bt_stops = pd.read_csv('../times_bt_stops.csv',
    dtype={
        'line': 'str',
        'direction': 'uint16',
        'st_hour': 'uint16',
        'end_hour': 'uint16',
        'stopA': 'uint16',
        'stopB': 'uint16',
        'bus': 'uint16',
        'trip_time':'float32',
        'api_trip_time':'float32'
    }
)
#Parse the dates
times_bt_stops['date'] = pd.to_datetime(times_bt_stops['date'], format='%Y-%m-%d')

#FUNCTIONS
def process_hour_df(line_df,hour) :
    '''
    Returns the dataframe with the headways between buses for the line and hour selected.

    Parameters
    -----------------
        line_df: Dataframe
        hour : int
    '''
    #Hour df
    hour_df = line_df.loc[line_df.datetime.dt.hour == hour]
    if hour_df.shape[0] == 0 :
        return pd.DataFrame([])
    #Line id
    line = hour_df.iloc[0].line

    #Stops of each line reversed
    stops1 = lines_collected_dict[line]['1']['stops'][-2::-1]
    stops2 = lines_collected_dict[line]['2']['stops'][-2::-1]

    #Assign destination values
    dest2,dest1 = lines_collected_dict[line]['destinations']

    #Dates range of iteration
    start_date = hour_df.datetime.min()
    end_date = hour_df.datetime.max()
    date = start_date

    #Rows list to build
    rows_list = []

    #Iterate over dates
    while date < end_date :
        date_hour_df = hour_df.loc[(hour_df.datetime.dt.year == date.year) & \
                                (hour_df.datetime.dt.month == date.month) & \
                                (hour_df.datetime.dt.day == date.day)]
        #Update date value for next iteration
        date = date + timedelta(days=1)

        #Process mean times between stops
        tims_bt_stops = times_bt_stops.loc[(times_bt_stops.line == line) & \
                                            (times_bt_stops.date.dt.weekday == date.weekday()) & \
                                            (times_bt_stops.st_hour == hour)]
        #Group and get the mean values
        tims_bt_stops = tims_bt_stops.groupby(['line','direction','stopA','stopB']).mean()
        tims_bt_stops = tims_bt_stops.reset_index()[['line','direction','stopA','stopB','trip_time','api_trip_time']]

        if date_hour_df.shape[0] > 0 :
            #First interval for the iteration :
            actual_date = date_hour_df.iloc[0].datetime
            start_interval = actual_date - timedelta(seconds=5)
            end_interval = actual_date + timedelta(seconds=15)

            #Iterate over bursts
            while True :
                int_df = date_hour_df.loc[(date_hour_df.datetime > start_interval) & (date_hour_df.datetime < end_interval)]

                if int_df.shape[0] > 0 :
                    #All stops of the line
                    stops = stops1 + stops2
                    stop_df_list = []
                    dest,direction = dest1,'1'
                    for i in range(len(stops)) :
                        stop = stops[i]
                        if i == 0 :
                            mean_time_to_stop = 0
                        elif i == len(stops1) :
                            mean_time_to_stop = 0
                            dest,direction = dest2,'2'
                        else :
                            mean_df = tims_bt_stops.loc[(tims_bt_stops.stopA == int(stop)) & \
                                            (tims_bt_stops.direction == int(direction))]
                            if mean_df.shape[0] > 0 :
                                mean_time_to_stop += mean_df.iloc[0].trip_time
                            else :
                                break

                        stop_df = int_df.loc[(int_df.stop == int(stop)) & \
                                            (int_df.destination == dest)]

                        #Drop duplicates, recalculate estimateArrive and append to list
                        stop_df = stop_df.drop_duplicates('bus',keep='first')
                        stop_df.estimateArrive = stop_df.estimateArrive + mean_time_to_stop
                        stop_df_list.append(stop_df)

                    #Concatenate and group them
                    stops_df = pd.concat(stop_df_list)

                    #Group by bus and destination
                    stops_df = stops_df.groupby(['bus','destination']).mean().sort_values(by=['estimateArrive'])
                    stops_df = stops_df.reset_index(drop=False).drop_duplicates('bus',keep='first')

                    #Calculate time intervals
                    if stops_df.shape[0] > 0 :
                        hw_pos1 = 0
                        hw_pos2 = 0
                        for i in range(stops_df.shape[0]-1) :
                            est1 = stops_df.iloc[i]
                            est2 = stops_df.iloc[i+1]

                            if est1.destination == est2.destination :
                                direction = '1' if est2.destination == dest1 else '2'
                                headway = int(est2.estimateArrive-est1.estimateArrive)

                                #Create dataframe row
                                row = {}
                                row['datetime'] = actual_date
                                row['line'] = line
                                row['direction'] = direction
                                row['busA'] = est1.bus
                                row['busB'] = est2.bus
                                row['hw_pos'] = hw_pos1 if direction == '1' else hw_pos2
                                row['headway'] = headway

                                #Append row to the list of rows
                                rows_list.append(row)

                                #Increment hw pos
                                if direction == '1' :
                                    hw_pos1 += 1
                                else :
                                    hw_pos2 += 1

                #Update iteration interval
                next_df = date_hour_df.loc[(date_hour_df.datetime > end_interval) & \
                                        (date_hour_df.datetime < end_interval + timedelta(minutes=5))]
                if next_df.shape[0] == 0 :
                    next_df = date_hour_df.loc[(date_hour_df.datetime > end_interval)]

                if next_df.shape[0] != 0 :
                    actual_date = next_df.iloc[0].datetime
                    start_interval = actual_date - timedelta(seconds=5)
                    end_interval = actual_date + timedelta(seconds=15)
                else :
                    break

    return pd.DataFrame(rows_list)

def get_headways(df) :
    '''
    Returns a dataframe with the headways between buses for every day, line and hour range.

    Parameters
    -----------------
        df: Dataframe
            Data to process
    '''
    #For every line collected
    lines = ['1','44','82','F','G','U','132','133','N2','N6']

    #Get new dictionaries and process the lines
    dfs_list = []
    for line in lines :
        hours = range(0,6) if line in ['N2','N6'] else range(7,23)
        line_df = df.loc[df.line == line]
        dfs = (Parallel(n_jobs=num_cores)(delayed(process_hour_df)(line_df,hour) for hour in hours))
        dfs_list += dfs

    #Concatenate dataframes
    processed_df = pd.concat(dfs_list).sort_values(by=['line','direction','datetime'],ascending=True).reset_index(drop=True)
    return processed_df

#MAIN

def main():
    # WE LOAD THE ARRIVAL TIMES DATA
    now = datetime.datetime.now()
    print('\n-------------------------------------------------------------------')
    print('Reading the original data... - {}\n'.format(now))
    buses_data = pd.read_csv('../buses_data_p.csv',
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
    )[['line','destination','stop','bus','datetime','estimateArrive']]

    #Parse the dates
    buses_data['datetime'] = pd.to_datetime(buses_data['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
    print(buses_data.info())
    lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    #Preprocess data; adds day_trip, arrival_time and calculated coordinates attributes
    now = datetime.datetime.now()
    print('-------------------------------------------------------------------')
    print('Processing headways between buses... - {}\n'.format(now))
    headways = get_headways(buses_data)
    print(headways.info())
    lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    #Processed data info
    f = '../headways.csv'
    now = datetime.datetime.now()
    print('-------------------------------------------------------------------')
    print('Writting new data to {}... - {}'.format(f,now))
    #Write result to file
    headways.to_csv(f)
    lapsed_seconds = round((datetime.datetime.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    print('New data is ready!\n')


if __name__== "__main__":
    main()
