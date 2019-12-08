import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import geopandas as gpd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    dcc.Input(id = 'input_origin', value = 'Origin ID', type = 'text'),
    dcc.Input(id = 'input_destination', value = 'Destination ID', type = 'text'),
    html.Div(id = 'output-graph')
    ])

# FUNCTIONS

yellow_taxis = pd.read_csv('../NYCYellowTaxiData/2018/yellow_tripdata_2018-02-duration.csv')

# Read location ID regions
taxi_zones = gpd.read_file("../taxi_zones/taxi_zones.shp")
taxi_zones = taxi_zones.loc[:,['location_i','zone', 'geometry']].rename(columns={"location_i": "LocationID"}).dissolve(by='LocationID').reset_index()

def mean_duration_each_hour(trip_time, origin_id = 1, destination_id = 1):
    #Now we are going to analyse the mean time for each hour of the day that we can spend in a fixed trip
    trip_time = trip_time.loc[:,['start_time','PULocationID','DOLocationID','duration']]
    #We select one trip
    trip_time = trip_time.loc[(trip_time['PULocationID']==origin_id) & (trip_time['DOLocationID']==destination_id)]
    #And we get only the data of interest
    trip_time = trip_time.loc[:,['start_time','duration']]
    #Nos quedamos solo con la hora del dia
    trip_time['start_time'] = pd.to_datetime(trip_time['start_time'])
    trip_time['start_time'] = trip_time['start_time'].dt.hour

    mean_duration = []

    for i in range(0,24):
        df = trip_time.loc[trip_time['start_time'] == i]
        mean_duration.append(df['duration'].mean())

    return pd.DataFrame({'mean_duration': mean_duration})


@app.callback(
    Output(component_id = 'output-graph', component_property = 'children'),
    [Input(component_id = 'input_origin', component_property = 'value'),Input(component_id = 'input_destination', component_property = 'value')]
)


# UPDATE Graph

def update_graph (input_origin_value,input_destination_value):
    try:
        origin_id = int(input_origin_value)
        if type(origin_id)==int :
            origin_name = taxi_zones.loc[taxi_zones['LocationID']==float(origin_id)].iloc[0]['zone']
        else :
            origin_id = 1
            origin_name = 'Unknown'

        destination_id = int(input_destination_value)
        if type(destination_id)==int :
            destination_name = taxi_zones.loc[taxi_zones['LocationID']==float(destination_id)].iloc[0]['zone']
        else :
            destination_id = 1
            destination_name = 'Unknown'

        mean_duration = mean_duration_each_hour(yellow_taxis,origin_id,destination_id)

        return dcc.Graph(
            id = 'example-graph',
            figure = {
                'data': [
                    {'x': mean_duration.index, 'y': mean_duration.mean_duration, 'type': 'bar', 'name': 'Mean_Duration'},
                ],
                'layout': dict(
                    title = 'Mean duration of the trip between {} and {}'.format(origin_name,destination_name),
                    xaxis={
                        'title': 'Hour of the day'
                    },
                    yaxis={
                        'title': 'Duration of the trip in minutes'
                    }
                )
            }
        )
    except:
        return 'Some error'


if __name__ == '__main__' :
    app.run_server(debug=True)
