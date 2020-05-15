import pandas as pd
import numpy as np

import math

from scipy import stats
from scipy.spatial.distance import mahalanobis
from scipy.stats.distributions import chi2

from datetime import datetime as dt
from datetime import timedelta

import json

import os.path

import time

from sys import argv


#Lines collected dictionary
with open('../../Data/Static/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)
#Models parameters dictionary
with open('../../Data/Anomalies/models_params.json', 'r') as f:
    models_params_dict = json.load(f)
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

#Bus and headways column names
bus_names_all = ['bus' + str(i) for i in range(1,8+2)]
hw_names_all = ['hw' + str(i) + str(i+1) for i in range(1,8+1)]


def clean_data(df) :
    def check_conditions(row) :
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
    mask = df.apply(check_conditions,axis=1)
    #Select rows that match the conditions
    df = df.loc[mask].reset_index(drop=True)
    #Return cleaned DataFrame
    return df


#For every burst of data:
def process_headways(int_df,day_type,hour_range) :
    rows_list = []
    hour_range = [int(hour) for hour in hour_range.split('-')]
    #Burst time
    actual_time = int_df.iloc[0].datetime

    #Line
    line = int_df.iloc[0].line

    #Stops of each line reversed
    stops1 = lines_collected_dict[line]['1']['stops'][-2::-1]
    stops2 = lines_collected_dict[line]['2']['stops'][-2::-1]

    #Assign destination values
    dest2,dest1 = lines_collected_dict[line]['destinations']

    #Process mean times between stops
    tims_bt_stops = times_bt_stops.loc[(times_bt_stops.date.dt.weekday.isin(day_type_dict[day_type])) & \
                                        (times_bt_stops.st_hour >= hour_range[0]) & \
                                        (times_bt_stops.st_hour < hour_range[1])]
    #Group and get the mean values
    tims_bt_stops = tims_bt_stops.groupby(['line','direction','stopA','stopB']).mean()
    tims_bt_stops = tims_bt_stops.reset_index()[['line','direction','stopA','stopB','trip_time','api_trip_time']]

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
        elif (stop == stops1[-2]) or (stop == stops2[-2]) :
            stop_dist = int(lines_collected_dict[line][str(direction)]['distances'][stop])
            buses_near = stop_df.loc[stop_df.DistanceBus > stop_dist/2]
            if buses_near.shape[0] > 0 :
                if direction == 1 :
                    buses_out1 += buses_near.bus.unique().tolist()
                else :
                    buses_out2 += buses_near.bus.unique().tolist()
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
    stops_df = pd.concat(stop_df_list)

    #Group by bus and destination
    stops_df = stops_df.groupby(['bus','destination']).mean().sort_values(by=['estimateArrive'])
    stops_df = stops_df.reset_index().drop_duplicates('bus',keep='first').sort_values(by=['destination'])
    #Loc buses not given by first stop
    stops_df = stops_df.loc[((stops_df.destination == dest1) & (~stops_df.bus.isin(buses_out1))) | \
                            ((stops_df.destination == dest2) & (~stops_df.bus.isin(buses_out2))) ]

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
                row['datetime'] = actual_time
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
                row['datetime'] = actual_time
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

    return pd.DataFrame(rows_list)


def get_ndim_hws (df,dim) :
    #Generate names for the columns of the dataframe to be built
    hw_names = ['hw' + str(i) + str(i+1) for i in range(1,dim+1)]
    bus_names = ['bus' + str(i) for i in range(1,dim+2)]

    #Columns to build dictionary
    columns = {}
    names = ['datetime'] + bus_names + hw_names
    for name in names:
        columns[name] = []
    
    if df.shape[0] < 1 :
        return pd.DataFrame(columns=names)
    
    #Unique datetime identifiers for the bursts
    burst_time = df.iloc[0].datetime

    burst_df1 = df.loc[(df.datetime == burst_time) & (df.direction == 1)].sort_values('hw_pos')
    burst_df2 = df.loc[(df.datetime == burst_time) & (df.direction == 2)].sort_values('hw_pos')

    for i in range(max(burst_df1.shape[0],burst_df2.shape[0]) - (dim-1)) :
        if i < (burst_df1.shape[0] - (dim-1)) :
            columns['datetime'].append(burst_time)
            columns[bus_names[0]].append(burst_df1.iloc[i].busA)
            for k in range(dim):
                columns[hw_names[k]].append(burst_df1.iloc[i+k].headway)
                columns[bus_names[k+1]].append(burst_df1.iloc[i+k].busB)

        if i < (burst_df2.shape[0] - (dim-1)) :
            columns['datetime'].append(burst_time)
            columns[bus_names[0]].append(burst_df2.iloc[i].busA)
            for k in range(dim):
                columns[hw_names[k]].append(burst_df2.iloc[i+k].headway)
                columns[bus_names[k+1]].append(burst_df2.iloc[i+k].busB)

    return pd.DataFrame(columns)


def process_hws_ndim_mh_dist(lines,day_type,hour_range,burst_df) :
    windows_dfs,headways_dfs = [],[]
    
    #Read dict
    with open('../../Data/Anomalies/hyperparams.json', 'r') as f:
        hyperparams = json.load(f)

    #For every line
    for line in lines :
        conf = hyperparams[line]['conf']

        #Process the headways
        line_df = burst_df.loc[burst_df.line == line]

        #Calculate headways and append them
        headways = process_headways(line_df,day_type,hour_range)
        headways['line'] = line
        headways_dfs.append(headways)
        
        if headways.shape[0] < 1 :
            continue
            
        #Eliminate hw_pos == 0
        hws = headways.loc[headways.hw_pos > 0]
        
        #Max dimensional model trained
        models = models_params_dict[line][day_type][hour_range]
        max_dim = models['max_dim']

        #Process windows dfs
        dim,window_data_points = 1,1
        while (window_data_points > 0) and (dim <= max_dim) :
            window_df = get_ndim_hws(hws,dim)
            window_data_points = window_df.shape[0]

            hw_names = ['hw' + str(i) + str(i+1) for i in range(1,dim+1)]
            bus_names = ['bus' + str(i) for i in range(1,dim+2)]

            hw_names_rest = ['hw' + str(i) + str(i+1) for i in range(dim+1,8+1)]
            bus_names_rest = ['bus' + str(i) for i in range(dim+2,8+2)]

            cov_matrix = models[str(dim)]['cov_matrix']
            mean = models[str(dim)]['mean']

            #Get Mahalanobis distance squared from which a certain percentage of the data is out
            m_th = math.sqrt(chi2.ppf(conf, df=dim))

            #Inverse of cov matrix
            if dim > 1:
                iv = np.linalg.inv(cov_matrix)

            def calc_m_dist(row) :
                row_hws = row[hw_names]
                if dim > 1 :
                    row['m_dist'] = round(mahalanobis(mean, row_hws, iv),5)
                else :
                    std = cov_matrix
                    hw_val = row_hws[hw_names[0]]
                    row['m_dist'] = round(np.abs(mean - hw_val)/std,5)

                if row['m_dist'] > m_th :
                    row['anom'] = 1
                else :
                    row['anom'] = 0

                return row

            if window_data_points > 0 :
                #Set columns values
                window_df = window_df.apply(calc_m_dist,axis=1)
                window_df['line'] = line
                window_df['dim'] = dim

                #Set to 0 unused columns
                for name in hw_names_rest + bus_names_rest :
                    window_df[name] = 0

                window_df = window_df[['line','datetime','dim','m_dist','anom'] + bus_names + bus_names_rest + hw_names + hw_names_rest]
                windows_dfs.append(window_df)

            #Increase dim
            dim += 1

    #Concat them in a dataframe
    headways_df = pd.concat(headways_dfs)

    if len(windows_dfs) > 0 :
        windows_df = pd.concat(windows_dfs)
    else :
        windows_df = pd.DataFrame(columns = ['line','datetime','dim','m_dist','anom'] + bus_names + bus_names_rest + hw_names + hw_names_rest)

    return headways_df,windows_df


def build_series_anoms(series_df,windows_df) :
    new_series,anomalies_dfs = [],[]
    dims = windows_df.dim.unique().tolist()
    for dim in dims :
        dim_df = windows_df.loc[windows_df.dim == dim]

        bus_names = ['bus' + str(i) for i in range(1,dim+2)]
        unique_groups = []
        for i in range(dim_df.shape[0]):
            group = [dim_df.iloc[i][bus_names[k]] for k in range(dim+1)]
            unique_groups.append(group)

        for group in unique_groups :
            #Build indexing condition
            conds1 = [dim_df[bus_names[k]] == group[k] for k in range(dim+1)]
            conds2 = [series_df[bus_names[k]] == group[k] for k in range(dim+1)]
            final_cond1,final_cond2 = True,True

            for i in range(len(conds1)) :
                final_cond1 &= conds1[i]
                final_cond2 &= conds2[i]

            #Current group status
            group_now = dim_df.loc[final_cond1].iloc[0]

            #Past group status
            group_df = series_df.loc[final_cond2]
            if group_df.shape[0] > 0 :
                group_df = group_df.sort_values('datetime',ascending=False)
                last_size = group_df.iloc[0].anom_size
                last_time = group_df.iloc[0].datetime
                seconds_ellapsed = (dt.now() - last_time).total_seconds()

                if ((group_now['anom'] == 0) and (last_size > 0)) |  ((group_now['anom'] == 1) and (seconds_ellapsed > 120)):
                    group_now['anom_size'] = 0

                    #We append the finished anomalies to the anomalies dfs
                    group_df['anom_size'] = last_size
                    anomalies_dfs.append(group_df.loc[group_df.anom == 1])
                    group_df['anom'] = 0
                else :
                    group_now['anom_size'] = last_size + group_now['anom']
            else :
                group_now['anom_size'] = group_now['anom']

            new_series.append(group_now)

    #Build current series dataframe and append it to the past series
    new_series_df = pd.DataFrame(new_series)[['line','datetime','dim','m_dist','anom','anom_size'] + bus_names_all + hw_names_all]

    series_df = series_df.append(new_series_df, ignore_index=True)

    return series_df,anomalies_dfs


def clean_series(series_df,anomalies_dfs,now) :
    unique_groups = []
    unique_groups_df = series_df.drop_duplicates(bus_names_all)
    for i in range(series_df.shape[0]):
        group = [series_df.iloc[i][bus_names_all[k]] for k in range(8+1)]
        unique_groups.append(group)

    for group in unique_groups :
        #Build indexing conditions
        conds = [series_df[bus_names_all[k]] == group[k] for k in range(8+1)]
        final_cond = True
        for cond in conds :
            final_cond &= cond
        group_df = series_df.loc[final_cond]

        if group_df.shape[0] > 0 :
            group_df = group_df.sort_values('datetime',ascending=False)
            last_time = group_df.iloc[0].datetime
            last_size = group_df.iloc[0].anom_size

            seconds_ellapsed = (now - last_time).total_seconds()

            if seconds_ellapsed > 300 :
                #Delete series from last series df
                series_df = series_df.loc[~final_cond]
                

                #If it was an anomaly
                if last_size > 0 :
                    group_df['anom_size'] = last_size
                    anomalies_dfs.append(group_df.loc[group_df.anom == 1])

    return series_df,anomalies_dfs


def detect_anomalies(burst_df,last_burst_df,series_df) :
    #Read dict
    with open('../../Data/Anomalies/hyperparams.json', 'r') as f:
        hyperparams = json.load(f)

    #Check if the burst dataframe has changed
    if last_burst_df.equals(burst_df) :
        #If it hasnt changed we return None
        return None

    #Lines to iterate over
    lines = ['1','44','82','132','133']
    #Day types to iterate over
    day_types = ['LA','SA','FE']
    #Hour ranges to iterate over
    #hour_ranges = [[7,11], [11,15], [15,19], [19,23]]
    hour_ranges = [[i,i+1] for i in range(7,23)]

    #Detect day type and hour range from current datetime
    now = dt.now()
    #Day type
    if (now.weekday() >= 0) and (now.weekday() <= 4) :
        day_type = 'LA'
    elif now.weekday() == 5 :
        day_type = 'SA'
    else :
        day_type = 'FE'
    #Hour range
    for h_range in hour_ranges :
        if (now.hour >= h_range[0]) and (now.hour < h_range[1]) :
            hour_range = str(h_range[0]) + '-' + str(h_range[1])
            break
        elif (h_range == hour_ranges[-1]) :
            print('\nHour range for {}:{} not defined. Waiting till 7am.\n'.format(now.hour,now.minute))
            return 'Wait'

    #Process headways and build dimensional dataframes
    headways_df,windows_df = process_hws_ndim_mh_dist(lines,day_type,hour_range,burst_df)

    #Check for anomalies and update the time series
    series_df,anomalies_dfs = build_series_anoms(series_df,windows_df)

    #Delete series from the list that havent appeared in the last 5 minutes
    series_df,anomalies_dfs = clean_series(series_df,anomalies_dfs,now)

    #Series df 
    series_df = series_df.reset_index(drop=True)
    
    #Build anomalies dataframe
    anomalies_df = pd.concat(anomalies_dfs).drop('anom',axis=1) if len(anomalies_dfs) > 0 else pd.DataFrame(columns = ['line','datetime','dim','m_dist','anom_size'] + bus_names_all + hw_names_all)
    
    #Get anomalies with size over threshold
    lines_anomalies = []
    for line in lines :
        line_anomalies = anomalies_df[anomalies_df.line == line]
        if line_anomalies.shape[0] > 0 :
            size_th = hyperparams[line]['size_th']
            line_anomalies = line_anomalies[line_anomalies.anom_size >= size_th]
            lines_anomalies.append(line_anomalies)
    try :
        anomalies_df = pd.concat(lines_anomalies)
    except :
        anomalies_df = pd.DataFrame()

    return headways_df,series_df,anomalies_df


def main():
    try :
        #Read last series data
        series_df = pd.read_csv('../../Data/RealTime/series.csv',
            dtype={
                'line': 'str'
            }
        )
        #Parse the dates
        series_df['datetime'] = pd.to_datetime(series_df['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
        series_df = series_df[['line','datetime','dim','m_dist','anom','anom_size'] + bus_names_all + hw_names_all]

        if (dt.now() - series_df.iloc[-1].datetime).total_seconds() > 600 :
            #Initialize dataframes
            series_df = pd.DataFrame(columns = ['line','datetime','dim','m_dist','anom','anom_size'] + bus_names_all + hw_names_all)
    
    except:
        #Initialize dataframes
        series_df = pd.DataFrame(columns = ['line','datetime','dim','m_dist','anom','anom_size'] + bus_names_all + hw_names_all)
    
    last_burst_df = pd.DataFrame(columns = ['line','destination','stop','bus','datetime','estimateArrive','DistanceBus'])

    #Inform of the selected parameters
    print('\n----- Detecting anomalies and preprocessing server data -----\n')

    #Look for updated data every 5 seconds
    while True :
        try :
            #Read last burst of data
            burst_df = pd.read_csv('../../Data/RealTime/buses_data_burst.csv',
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
            burst_df['datetime'] = pd.to_datetime(burst_df['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
            
            #Clean burst df
            burst_df = clean_data(burst_df)
        except :
            print('\n---- Last burst of data not found, waiting... -----\n')
            time.sleep(10)
            continue
        
        #try :
        result = detect_anomalies(burst_df,last_burst_df,series_df)
        #except :
            #result = None
        
        #If the data was updated write files
        if result :
            if len(result) == 3 :
                headways_df,series_df,anomalies_df = result

                #print(headways_df.head(2))
                #print(series_df.head(2))
                #print(anomalies_df.head(2))

                #Write new data to files
                f = '../../Data/'
                burst_df.to_csv(f+'RealTime/buses_data_burst_c.csv')
                
                headways_df.to_csv(f+'RealTime/headways_burst.csv')

                series_df.to_csv(f+'RealTime/series.csv')

                if anomalies_df.shape[0] > 0 :
                    if os.path.isfile(f+'Anomalies/anomalies.csv') :
                        anomalies_df.to_csv(f+'Anomalies/anomalies.csv', mode='a', header=False)
                    else :
                        anomalies_df.to_csv(f+'Anomalies/anomalies.csv', mode='a', header=True)

                print('\n----- Burst headways and series were processed. Anomalies detected were added - {} -----\n'.format(dt.now()))
        
            elif result == 'Wait' :
                time.sleep(120)

        last_burst_df = burst_df
        time.sleep(5)

if __name__== "__main__":
    main()