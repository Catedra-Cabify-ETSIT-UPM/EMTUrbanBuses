import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import geopandas as gpd

from taxis_data import yellow_taxis

app = dash.Dash()

app.layout = html.Div(children=[
    dcc.Input(id = 'input', value = 'Enter something', type = 'text'),
    html.Div(id = 'output-graph')
    ])

@app.callback(
    Output(component_id = 'output-graph', component_property = 'children'),
    [Input(component_id = 'input', component_property = 'value')]
)

# FUNCTIONS

def mean_duration_each_hour(trip_time,origin_id,destination_id):
    if !origin_id :
        origin_id = 1
    if !destination_id :
        destination_id = 1

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

    for i in range(0,23):
        df = trip_time.loc[trip_time['start_time'] == i]
        mean_duration.append(df['duration'].mean())

    return pd.DataFrame({'mean_duration': mean_duration})

# UPDATE Graph

def update_graph (input_data):
    try :
        origin_id = input_data
        destination_id = input_data
        mean_duration = mean_duration_each_hour(yellow_taxis,origin_id,destination_id)

        return dcc.Graph(
            id = 'example-graph',
            figure = {
                'data': [
                    {'x': mean_duration.index, 'y': mean_duration.mean_duration, 'type': 'bar', 'name': 'Mean_Duration'},
                ],
                'layout': {
                    'title': 'Mean duration of the trip between {} and {}'.format(origin_id,destination_id)
                }
            }
        )
    except :
        return "Some error"

if __name__ == '__main__' :
    app.run_server(debug=True)
