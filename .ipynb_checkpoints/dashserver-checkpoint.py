import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import geopandas as gpd

import calendar as calendar

#We setup the app
app = dash.Dash(__name__)

#We import the layout from another file
from html_layout import index_string
app.index_string = index_string

#SERVER LAYOUT FOR THE INTERACTIVE ELEMENTS
app.layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('Location ID to Name translator',className = 'title is-3'),
        html.Span('Location ID: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Location ID', id = 'input_location_id', value = '', type = 'text'),
        html.Div(id = 'output-location_name', className = 'box')
    ]),

    html.Div(className = 'box', children = [
        html.H1('Mean duration of the trip between two given locations',className = 'title is-3'),
        html.Span('Origin ID: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Origin ID', id = 'input_origin', value = '', type = 'text'),
        html.Span('Destination ID: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Destination ID', id = 'input_destination', value = '', type = 'text'),
        html.Div(id = 'output-graph', className = 'box')
    ]),

    html.Div(className = 'box', children = [
        html.H1('Number of pick ups for each day of the month for two given locations',className = 'title is-3'),
        html.Span('Location 1: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Location 1 ID', id = 'input_loc1', value = '', type = 'text'),
        html.Span('Location 2: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Location 2 ID', id = 'input_loc2', value = '', type = 'text'),
        html.Span('Year of Interest: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Year of Interest', id = 'input_year', value = '', type = 'text'),
        html.Span('Month of Interest: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Enter Month of Interest', id = 'input_month', value = '', type = 'text'),
        html.Div(id = 'output-graph2', className = 'box')
    ])

])

#WE READ THE DATASETS OF INTEREST
#Yellow taxis dataset
yellow_taxis = pd.read_csv('../flash/NYCTaxiData/yellow_taxi/2018/yellow_tripdata_2018-02-duration.csv')

#Locations dataset
taxi_zones = gpd.read_file("../taxi_zones/taxi_zones.shp")
taxi_zones = taxi_zones.loc[:,['location_i','zone', 'geometry']].rename(columns={"location_i": "LocationID"}).dissolve(by='LocationID').reset_index()

#FUNCTIONS

#Mean duration of the trip between two locations for each hour
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

#Pickups for each location each day of the month
def pickups_month(time_pickups, loc1_id, loc2_id, year, month) :
    #We addapt the data to plot the pick ups in East Village and the JFK Airport over 2018.
    time_pickups = time_pickups.loc[:,['start_time','PULocationID']].rename(columns={'start_time':'date'})
    time_pickups['date'] = pd.to_datetime(time_pickups['date'])

    #We delete the incorrect dates from the data and the hour information(we want to group it by days)
    time_pickups = time_pickups.loc[(time_pickups['date'].dt.year == year) & (time_pickups['date'].dt.month == month)]
    time_pickups['date'] = time_pickups['date'].dt.day


    #We get the numbers of pickups per day
    time_pickups_loc1 = time_pickups.loc[time_pickups['PULocationID']==float(loc1_id)]['date'].value_counts()
    time_pickups_loc2 = time_pickups.loc[time_pickups['PULocationID']==float(loc2_id)]['date'].value_counts()

    time_pickups_final = pd.DataFrame({'loc1_pus': time_pickups_loc1,'loc2_pus':time_pickups_loc2, 'date':time_pickups_loc1.index}).set_index('date').sort_values(by ='date' )

    return time_pickups_final



#CALLBACK 1
@app.callback(
    Output(component_id = 'output-location_name', component_property = 'children'),
    [Input(component_id = 'input_location_id', component_property = 'value')]
)



#UPDATE Location Name
def update_location_name (input_location_id) :
    try:
        origin_id = int(input_location_id)
        return 'The location with ID {} is: {}'.format(input_location_id,taxi_zones.loc[taxi_zones['LocationID']==float(origin_id)].iloc[0]['zone'])
    except:
        return 'Please use integer values for the location IDs'


#CALLBACK 2
@app.callback(
    Output(component_id = 'output-graph', component_property = 'children'),
    [Input(component_id = 'input_origin', component_property = 'value'),Input(component_id = 'input_destination', component_property = 'value')]
)

#Update Mean Duration of the Trip Graph
def update_mean_duration_graph (input_origin_value,input_destination_value):
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
            id = 'graph1',
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
        return 'Please use integer values for the location IDs'

#CALLBACK 3
@app.callback(
    Output(component_id = 'output-graph2', component_property = 'children'),
    [
        Input(component_id = 'input_loc1', component_property = 'value'),
        Input(component_id = 'input_loc2', component_property = 'value'),
        Input(component_id = 'input_year', component_property = 'value'),
        Input(component_id = 'input_month', component_property = 'value')
    ]
)

def update_pickups_month_graph (input_loc1_value,input_loc2_value,input_year_value,input_month_value) :
    try:
        year = int(input_year_value)
        month = int(input_month_value)
        month_name = calendar.month_name[month]

        loc1_id = int(input_loc1_value)
        if type(loc1_id)==int :
            loc1_name = taxi_zones.loc[taxi_zones['LocationID']==float(loc1_id)].iloc[0]['zone']
        else :
            loc1_id = 1
            loc1_name = 'Unknown'

        loc2_id = int(input_loc2_value)
        if type(loc2_id)==int :
            loc2_name = taxi_zones.loc[taxi_zones['LocationID']==float(loc2_id)].iloc[0]['zone']
        else :
            loc2_id = 1
            loc2_name = 'Unknown'

        time_pickups = pickups_month(yellow_taxis,loc1_id,loc2_id,year,month)

        return dcc.Graph(
            id = 'graph2',
            figure = {
                'data': [
                    {'x': time_pickups.index, 'y': time_pickups.loc1_pus, 'mode': 'lines+markers', 'name': '{}'.format(loc1_name)},
                    {'x': time_pickups.index, 'y': time_pickups.loc2_pus, 'mode': 'lines+markers', 'name': '{}'.format(loc2_name)}
                ],
                'layout': dict(
                    title = '{}-{} | Pickups in {} and {}'.format(month_name,year,loc1_name,loc2_name),
                    xaxis={
                        'title': 'Day of {}'.format(month_name)
                    },
                    yaxis={
                        'title': 'Number of pickups'
                    }
                )
            }
        )
    except:
        return 'Please use integer values for the location IDs and the dates settings'



#START THE SERVER
if __name__ == '__main__' :
    app.run_server(debug=True)
