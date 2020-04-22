import pandas as pd
import numpy as np

from datetime import datetime as dt
from datetime import timedelta

import json

#Day types
day_type_dict = { #0 = Monday, 1 = Tuesday ...
    'LA' : [0,1,2,3,4], #LABORABLES
    'LJ' : [0,1,2,3], #LUNES A JUEVES
    'VV' : [4], #VIERNES
    'SA' : [5], #SABADOS
    'FE' : [6], #DOMIGOS O FESTIVOS
}

#FUNCTIONS
def get_ndim_hws (df,dim) :
    #Generate names for the columns of the dataframe to be built
    hw_names = ['hw' + str(i) + str(i+1) for i in range(1,dim+1)]
    bus_names = ['bus' + str(i) for i in range(1,dim+2)]

    #Columns to build dictionary
    columns = {}
    names = ['datetime'] + bus_names + hw_names
    for name in names:
        columns[name] = []

    #Unique datetime identifiers for the bursts
    burst_times = df.datetime.unique()
    for burst_time in burst_times :
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


def get_model_params(df,dim) :
    cov_matrixes,means = [],[]
    #Locate only data columns
    dim_df = df.iloc[:,-dim:]
    #Check if dataframes dimensions match or if they are empty
    if dim_df.shape[1] != dim :
        return 'DataFrame index {} has {} dimensions, {} expected.'.format(dim-1,dim_df.shape[1],dim)
    if dim_df.shape[0] < 1 :
        return 'DataFrame index {} is empty'.format(dim-1)

    #Get columns from df
    cols = [dim_df.iloc[:,k] for k in range(dim)]

    #Get the covariance matrix of the data
    if dim > 1 :
        cov_matrix = np.cov(np.stack(cols, axis = 0))
    else :
        cov_matrix = cols[0].std()

    #Get mean of the data points
    if dim > 1 :
        mean = [col.mean() for col in cols]
    else :
        mean = cols[0].mean()

    return cov_matrix,mean


def train_models(df,min_points):
    models_params_dict = {}

    #Locate headways of the last three weeks
    now = dt.now()
    df = df.loc[df.datetime > (now - timedelta(days=21))]

    #Lines to iterate over
    lines = ['1','44','82','132','133']
    #Day types to iterate over
    day_types = ['LA','SA','FE']
    #Hour ranges to iterate over
    hour_ranges = [[7,11], [11,15], [15,19], [19,23]]

    #Train a model for each interval
    for line in lines :
        models_params_dict[line] = {}
        for day_type in day_types :
            models_params_dict[line][day_type] = {}
            for hour_range in hour_ranges:
                models_params_dict[line][day_type][str(hour_range[0])+'-'+str(hour_range[1])] = {}

                #Get data split
                split_df = df.loc[
                    (df.line == line) & \
                    (df.datetime.dt.weekday.isin(day_type_dict[day_type])) & \
                    (df.datetime.dt.hour >= hour_range[0]) & \
                    (df.datetime.dt.hour < hour_range[1])
                ]

                #Generate ndimensional windows dataframes
                window_data_points = min_points + 1
                dim = 1
                windows_dfs = []
                while window_data_points > min_points :
                    window_df = get_ndim_hws(split_df,dim)
                    window_data_points = window_df.shape[0]
                    dim += 1

                    if window_data_points > min_points :
                         windows_dfs.append(window_df)

                #Max dimension for which we are going to train a model
                max_dim = len(windows_dfs)
                models_params_dict[line][day_type][str(hour_range[0])+'-'+str(hour_range[1])]['max_dim'] = max_dim

                #For every window data
                for dim in range(1,max_dim+1) :
                    models_params_dict[line][day_type][str(hour_range[0])+'-'+str(hour_range[1])][dim] = {}

                    #Calculate cov_matrix and mean and add it to the dictionary
                    cov_matrix,mean = get_model_params(windows_dfs[dim-1],dim)

                    models_params_dict[line][day_type][str(hour_range[0])+'-'+str(hour_range[1])][dim]['cov_matrix'] = cov_matrix.tolist()
                    models_params_dict[line][day_type][str(hour_range[0])+'-'+str(hour_range[1])][dim]['mean'] = mean

    return models_params_dict



def main():
    # WE LOAD THE ARRIVAL TIMES DATA
    now = dt.now()
    print('\n-------------------------------------------------------------------')
    print('Reading the headways data... - {}\n'.format(now))
    #Headways data
    hws = pd.read_csv('../../Data/Processed/headways.csv',
        dtype={
            'line': 'str',
            'direction': 'uint16',
            'busA': 'uint16',
            'busB': 'uint16',
            'headway':'uint16',
            'busB_ttls':'uint16'
        }
    )[['line','direction','datetime','hw_pos','busA','busB','headway','busB_ttls']]
    #Parse the dates
    hws['datetime'] = pd.to_datetime(hws['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
    #Eliminate 0 pos from headways
    hws = hws.loc[(hws.hw_pos != 0)]
    print(hws.info())
    lapsed_seconds = round((dt.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    #Preprocess data; adds day_trip, arrival_time and calculated coordinates attributes
    now = dt.now()
    print('-------------------------------------------------------------------')
    print('Processing models parameters... - {}\n'.format(now))

    models_params_dict = train_models(hws,100)

    lapsed_seconds = round((dt.now()-now).total_seconds(),3)
    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    #Processed data info
    f = '../../Data/Anomalies/models_params.json'
    now = dt.now()
    print('-------------------------------------------------------------------')
    print('Writting models parameters to {}... - {}'.format(f,now))

    #Write result to file
    with open(f, 'w') as fp:
        json.dump(models_params_dict, fp)

    print('\nFinished in {} seconds'.format(lapsed_seconds))
    print('-------------------------------------------------------------------\n\n')

    print('New data is ready!\n')


if __name__== "__main__":
    main()
