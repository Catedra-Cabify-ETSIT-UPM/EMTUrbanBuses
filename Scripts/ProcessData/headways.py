import pandas as pd
import json

import datetime
from datetime import timedelta

from sys import argv

from joblib import Parallel, delayed
import multiprocessing
num_cores = multiprocessing.cpu_count()

#Load line_stops_dict
with open('../../Data/Static/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)
#Load times between stops data
times_bt_stops = pd.read_csv('../../Data/Processed/times_bt_stops.csv',
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

#Day types
day_type_dict = { #0 = Monday, 1 = Tuesday ...
    'LA' : [0,1,2,3,4], #LABORABLES
    'LJ' : [0,1,2,3], #LUNES A JUEVES
    'VV' : [4], #VIERNES
    'SA' : [5], #SABADOS
    'FE' : [6], #DOMIGOS O FESTIVOS
}

#FUNCTIONS
def process_day_df(line_df,date) :
    '''
    Returns the dataframe with the headways between buses for the line and day selected.

    Parameters
    -----------------
        line_df: Dataframe
        year,month,day : ints
    '''
    #Select day data
    day_df = line_df.loc[line_df.datetime.dt.date == date].sort_values(by='datetime',ascending=True)

    if day_df.shape[0] == 0 :
        return pd.DataFrame([])
    #Line id
    line = day_df.iloc[0].line

    now = day_df.datetime.iloc[0]
    #Day type
    if (now.weekday() >= 0) and (now.weekday() <= 4) :
        day_type = 'LA'
    elif now.weekday() == 5 :
        day_type = 'SA'
    else :
        day_type = 'FE'

    #Stops of each line reversed
    stops1 = lines_collected_dict[line]['1']['stops'][-2::-1][0:-2]
    stops2 = lines_collected_dict[line]['2']['stops'][-2::-1][0:-2]

    #Assign destination values
    dest2,dest1 = lines_collected_dict[line]['destinations']

    #Appearance order buses list
    ap_order_dir1,ap_order_dir2 = [],[]
    last_bus_ap1,last_bus_ap2 = {},{}
    bus_cons_ap1,bus_cons_ap2 = {},{}

    rows_list = []
    if day_df.shape[0] > 0 :
        #First interval for the iteration :
        actual_date = day_df.iloc[0].datetime
        start_interval = actual_date - timedelta(seconds=10)
        end_interval = actual_date + timedelta(seconds=30)

        #Iterate over bursts
        last_hour = -10
        while True :
            int_df = day_df.loc[(day_df.datetime > start_interval) & (day_df.datetime < end_interval)]

            if actual_date.hour > last_hour :
                last_hour = actual_date.hour
                #Process mean times between stops
                tims_bt_stops = times_bt_stops.loc[(times_bt_stops.line == line) & \
                                                    (times_bt_stops.date.dt.weekday.isin(day_type_dict[day_type])) & \
                                                    (times_bt_stops.st_hour >= last_hour) & \
                                                    (times_bt_stops.st_hour < last_hour+1)]
                #Group and get the mean values
                tims_bt_stops = tims_bt_stops.groupby(['line','direction','stopA','stopB']).mean()
                tims_bt_stops = tims_bt_stops.reset_index()[['line','direction','stopA','stopB','trip_time','api_trip_time']]

            if int_df.shape[0] > 0 :
                #All stops of the line
                stops = stops1 + stops2
                stop_df_list = []
                buses_out1,buses_out2 = [],[]
                dest,direction = dest1,1
                for i in range(len(stops)) :
                    stop = stops[i]
                    if i == 0 :
                        mean_time_to_stop = 0
                    elif i == len(stops1) :
                        mean_time_to_stop = 0
                        dest,direction = dest2,2
                    else :
                        mean_df = tims_bt_stops.loc[(tims_bt_stops.stopA == int(stop)) & \
                                        (tims_bt_stops.direction == direction)]
                        if mean_df.shape[0] > 0 :
                            mean_time_to_stop += mean_df.iloc[0].trip_time
                        else :
                            break

                    stop_df = int_df.loc[(int_df.stop == int(stop)) & \
                                        (int_df.destination == dest)]

                    #Drop duplicates, recalculate estimateArrive and append to list
                    stop_df = stop_df.drop_duplicates('bus',keep='first')

                    if (stop == stops1[-1]) or (stop == stops2[-1]) :
                        if direction == 1 :
                            buses_out1 += stop_df.bus.unique().tolist()
                        else :
                            buses_out2 += stop_df.bus.unique().tolist()
                    elif (stop == stops1[0]) or (stop == stops2[0]) :
                        buses_near = stop_df.loc[stop_df.estimateArrive < 20]
                        if buses_near.shape[0] > 0 :
                            if direction == 1 :
                                buses_out1 += buses_near.bus.unique().tolist()
                            else :
                                buses_out2 += buses_near.bus.unique().tolist()

                    stop_df.estimateArrive = stop_df.estimateArrive + mean_time_to_stop
                    stop_df_list.append(stop_df)

                #Concatenate and group them
                if len(stop_df_list)>0:
                    stops_df = pd.concat(stop_df_list)

                    #Group by bus and destination
                    stops_df = stops_df.groupby(['bus','destination']).mean()
                    stops_df = stops_df.reset_index().drop_duplicates('bus',keep='first')
                    #Loc buses not given by first stop
                    stops_df = stops_df.loc[((stops_df.destination == dest1) & (~stops_df.bus.isin(buses_out1))) | \
                                            ((stops_df.destination == dest2) & (~stops_df.bus.isin(buses_out2))) ]

                    #Update appearance order lists
                    stops_df_dest1 = stops_df[stops_df.destination == dest1].sort_values(by=['estimateArrive'])
                    if stops_df_dest1.shape[0] > 0 :  
                        buses_dest1 = stops_df_dest1.bus.tolist()
                        for i in range(len(buses_dest1)):
                            if buses_dest1[i] not in bus_cons_ap1.keys() :
                                bus_cons_ap1[buses_dest1[i]] = 0
                            if  buses_dest1[i] not in last_bus_ap1.keys() :
                                last_bus_ap1[buses_dest1[i]] = 0

                            if bus_cons_ap1[buses_dest1[i]] > 1 :
                                if (buses_dest1[i] not in ap_order_dir1) : 
                                    #Append to apearance list
                                    ap_order_dir1.append(buses_dest1[i])

                                elif last_bus_ap1[buses_dest1[i]] > 1 :
                                    for k in range(len(ap_order_dir1)) :
                                        if buses_dest1[i] == ap_order_dir1[k] :
                                            #Put it in the last position
                                            ap_order_dir1.append(ap_order_dir1.pop(k))
                                            break

                        #Update times without appering
                        for bus in last_bus_ap1.keys():
                            if bus not in buses_dest1 :
                                last_bus_ap1[bus] += 1
                                bus_cons_ap1[bus] = 0
                                if last_bus_ap1[bus] > 1 :
                                    if bus in ap_order_dir1 :
                                        ap_order_dir1.remove(bus)
                            else :
                                last_bus_ap1[bus] = 0
                                bus_cons_ap1[bus] += 1
                    
                    stops_df_dest2 = stops_df[stops_df.destination == dest2].sort_values(by=['estimateArrive'])
                    if stops_df_dest2.shape[0] > 0 :  
                        buses_dest2 = stops_df_dest2.bus.tolist()
                        for i in range(len(buses_dest2)):
                            if buses_dest2[i] not in bus_cons_ap2.keys() :
                                bus_cons_ap2[buses_dest2[i]] = 0
                            if  buses_dest2[i] not in last_bus_ap2.keys() :
                                last_bus_ap2[buses_dest2[i]] = 0

                            if bus_cons_ap2[buses_dest2[i]] > 1 :
                                if (buses_dest2[i] not in ap_order_dir2) : 
                                    #Append to apearance list
                                    ap_order_dir2.append(buses_dest2[i])

                                elif last_bus_ap2[buses_dest2[i]] > 1 :
                                    for k in range(len(ap_order_dir2)) :
                                        if buses_dest2[i] == ap_order_dir2[k] :
                                            #Put it in the last position
                                            ap_order_dir2.append(ap_order_dir2.pop(k))
                                            break

                        #Update times without appering
                        for bus in last_bus_ap2.keys():
                            if bus not in buses_dest2 :
                                last_bus_ap2[bus] += 1
                                bus_cons_ap2[bus] = 0
                                if last_bus_ap2[bus] > 1 :
                                    if bus in ap_order_dir2 :
                                        ap_order_dir2.remove(bus)
                            else :
                                last_bus_ap2[bus] = 0
                                bus_cons_ap2[bus] += 1

                    #Reorder df according to appearance list
                    rows,last_ttls1,last_bus1,last_ttls2,last_bus2 = [],0,0,0,0
                    for bus in ap_order_dir1 :
                        bus_df = stops_df_dest1[stops_df_dest1.bus == bus]
                        if bus_df.shape[0] > 0 :
                            if (last_ttls1 > bus_df.iloc[0].estimateArrive) & (bus_cons_ap1[bus] == 3) :
                                popped_row = rows.pop(-1)
                                rows.append(bus_df.iloc[0])
                                rows.append(popped_row)

                                ap_order_dir1.remove(last_bus1)
                                bus_cons_ap1[last_bus1] = 2
                                last_bus_ap1[last_bus1] = 2
                                break
                            else :
                                rows.append(bus_df.iloc[0])
                                last_ttls1 = bus_df.iloc[0].estimateArrive
                                last_bus1 = bus
                        
                    for bus in ap_order_dir2 :
                        bus_df = stops_df_dest2[stops_df_dest2.bus == bus]
                        if bus_df.shape[0] > 0 :
                            if (last_ttls2 > bus_df.iloc[0].estimateArrive) & (bus_cons_ap2[bus] == 3) :
                                popped_row = rows.pop(-1)
                                rows.append(bus_df.iloc[0])
                                rows.append(popped_row)

                                ap_order_dir2.remove(last_bus2)
                                bus_cons_ap2[last_bus2] = 2
                                last_bus_ap2[last_bus2] = 2
                                break
                            else :
                                rows.append(bus_df.iloc[0])
                                last_ttls2 = bus_df.iloc[0].estimateArrive
                                last_bus2 = bus

                    #Rebuild df
                    stops_df = pd.DataFrame(rows)

                    #Calculate time intervals
                    if stops_df.shape[0] > 0 :
                        hw_pos1 = 0
                        hw_pos2 = 0
                        for i in range(stops_df.shape[0]) :
                            est1 = stops_df.iloc[i]

                            direction = 1 if est1.destination == dest1 else 2
                            if ((direction == 1) & (hw_pos1 == 0)) or ((direction == 2) & (hw_pos2 == 0))  :
                                #Create dataframe row
                                row = {}
                                row['datetime'] = actual_date
                                row['line'] = line
                                row['direction'] = direction
                                row['busA'] = 0
                                row['busB'] = est1.bus
                                row['hw_pos'] = 0
                                row['headway'] = 0
                                row['busB_ttls'] = int(est1.estimateArrive)

                                #Append row to the list of rows
                                rows_list.append(row)

                                #Increment hw pos
                                if direction == 1 :
                                    hw_pos1 += 1
                                else :
                                    hw_pos2 += 1

                            if i < (stops_df.shape[0] - 1) :
                                est2 = stops_df.iloc[i+1]
                            else :
                                break

                            if est1.destination == est2.destination :
                                headway = int(est2.estimateArrive-est1.estimateArrive)

                                #Create dataframe row
                                row = {}
                                row['datetime'] = actual_date
                                row['line'] = line
                                row['direction'] = direction
                                row['busA'] = est1.bus
                                row['busB'] = est2.bus
                                row['hw_pos'] = hw_pos1 if direction == 1 else hw_pos2
                                row['headway'] = headway
                                row['busB_ttls'] = int(est2.estimateArrive)

                                #Append row to the list of rows
                                rows_list.append(row)

                                #Increment hw pos
                                if direction == 1 :
                                    hw_pos1 += 1
                                else :
                                    hw_pos2 += 1

            #Update iteration interval
            next_df = day_df.loc[(day_df.datetime > end_interval) & \
                                    (day_df.datetime < end_interval + timedelta(minutes=5))]
            if next_df.shape[0] == 0 :
                next_df = day_df.loc[(day_df.datetime > end_interval)]

            if next_df.shape[0] != 0 :
                actual_date = next_df.iloc[0].datetime
                start_interval = actual_date - timedelta(seconds=10)
                end_interval = actual_date + timedelta(seconds=30)
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
        line_df = df.loc[df.line == line]
        dates = line_df.datetime.dt.date.unique().tolist()
        dfs = (Parallel(n_jobs=num_cores,max_nbytes=None)(delayed(process_day_df)(line_df,date) for date in dates))
        dfs_list += dfs

    #Concatenate dataframes
    processed_df = pd.concat(dfs_list).sort_values(by=['line','datetime','direction'], ascending=True).reset_index(drop = True)
    return processed_df

#MAIN

def main():
    # WE LOAD THE ARRIVAL TIMES DATA
    now = datetime.datetime.now()
    print('\n-------------------------------------------------------------------')
    print('Reading the data cleaned with the arrival times included... - {}\n'.format(now))
    buses_data = pd.read_csv('../../Data/Processed/buses_data_pc.csv',
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
    )[['line','destination','stop','bus','datetime','estimateArrive','DistanceBus']]

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
    f = '../../Data/Processed/headways.csv'
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
