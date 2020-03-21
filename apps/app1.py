import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
import json

import plotly.graph_objects as go
import math

import asyncio
from concurrent.futures import ThreadPoolExecutor
import datetime

from app import app



# WE LOAD THE DATA
stops = pd.read_csv('M6Data/stops.csv')
lines_shapes = pd.read_csv('M6Data/lines_shapes.csv')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)


layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('LIVE POSITION OF BUSES BELONGING TO DESIRED LINE',className = 'title is-3'),
        html.Span('Line ID: ', className = 'tag is-light is-large'),
        dcc.Dropdown(
            id="lineId-select",
            options=[{"label": i, "value": i} for i in line_stops_dict.keys()],
            value='1',
            searchable=True
        ),
        html.Div(className='box',id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=10*1000, # in milliseconds
            n_intervals=0
        )
    ])

])

#MAPBOX API TOKEN AND STYLES
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style_day = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
style_night = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'


# API FUNCTIONS
def requests_retry_session(retries=3,backoff_factor=0.3,status_forcelist=(500, 502, 504),session=None):
    '''
    Function to ensure we get a good response for the request
    '''
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_access_token(email,password) :
    '''
    Returns the access token of the EMT Madrid API

        Parameters
        ----------
        email : string
            The email of the account
        password : string
            Password of the account
    '''
    try:
        if email == 'a.jarabo@alumnos.upm.es' :
            #Special request for the account with API
            response = requests_retry_session().get(
                'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
                headers={
                    'X-ClientId':XClientId,
                    'passKey':passKey
                },
                timeout=5
            )
        else :
            response = requests_retry_session().get(
                'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
                headers={
                    'email':email,
                    'password':password
                },
                timeout=5
            )
        json_response = response.json()
        if json_response['code'] == '98' :
            return '98'
        else :
            return json_response['data'][0]['accessToken']

    except requests.exceptions.RequestException as e:
        print(e + '\n')
        return 'Error'

def get_arrival_times(lineId,stopId,accessToken) :
    """
    Returns the arrival data of buses for the desired stop and line

        Parameters
        ----------
        lineId : string
            The line id
        stopId : string
            The stop code
        accessToken: string
            The accessToken obtained in the login
    """

    #We build the body for the request
    body = {
        'cultureInfo': 'ES',
        'Text_StopRequired_YN': 'N',
        'Text_EstimationsRequired_YN': 'Y',
        'Text_IncidencesRequired_YN': 'N',
        'DateTime_Referenced_Incidencies_YYYYMMDD':'20200130'
    }

    #And we perform the request
    try:
        response = requests_retry_session().post(
            'https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{}/arrives/{}/'.format(stopId,lineId),
            data = json.dumps(body),
            headers = {
                'accessToken': accessToken,
                'Content-Type': 'application/json'
            },
            timeout = 20
        )
        #Return the response if we received it ok
        return response
    except requests.exceptions.RequestException as e:
        print(e + '\n')
        return 'Error'


# WE LOGIN IN THE EMT API
from api_credentials import emails,passwords,XClientId,passKey
#Try to get an accessToken until we get a response to the login without errors
for i in range(0,5) :
    accessToken = 'Error'
    while accessToken == 'Error' :
        accessToken = get_access_token(emails[i],passwords[i])
    if accessToken == '98' :
        if i == 4 :
            print('No hits available in any of the accounts - Closing server')
            exit(0)
    else :
        #Login made correctly
        break


# FUNCTIONS
def haversine(coord1, coord2):
    '''
    Returns distance between two given coordinates in meters
    '''
    R = 6372800  # Earth radius in meters
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + \
        math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2

    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def find_nearest_row(df,lat,lon) :
    """
    Returns the row nearest to the coordinates passed in the dataframe

        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        lat: float
        lon: float
    """
    min_dist_error = 1000000.0
    for row in df.itertuples() :
        error = math.sqrt(abs(row.lat-lat)+abs(row.lon-lon))
        if  error < min_dist_error :
            min_dist_error = error
            nearest_row = row
    return nearest_row

def find_nearest_row_by_dist(df,dist_traveled) :
    """
    Returns the row nearest to the distance traveled passed in the dataframe

        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        dist_traveled : float
    """
    min_dist_error = 1000000.0
    for row in df.itertuples() :
        error = abs(row.dist_traveled-dist_traveled)
        if  error < min_dist_error :
            min_dist_error = error
            nearest_row = row
    return nearest_row

def point_by_distance_on_line (line,distance,stop_lat,stop_lon) :
    """
    Returns the coordinates of the bus location

        Parameters
        ----------
        line: DataFrame
            Points belonging to the requested line
        distance : float
            Distance of the bus to the stop in meters
        stop_lat : float
        stop_lon : float
    """

    nearest_row_to_stop = find_nearest_row(line,stop_lat,stop_lon)
    dist_traveled_of_bus = nearest_row_to_stop.dist_traveled - distance
    nearest_row_to_distance = find_nearest_row_by_dist(line,dist_traveled_of_bus)

    #And we return the coordinates of the point
    return nearest_row_to_distance.lon,nearest_row_to_distance.lat

def get_arrival_time_data_of_line(line1,line2,accessToken) :
    """
    Returns the data of all the buses inside the desired line

        Parameters
        ----------
        line1 : DataFrame
            Data about the line in direction 1
        line2 : DataFrame
            Data about the line in direction 2
        accessToken: string
            The accessToken obtained in the login
    """
    #id and short name of the line used for the requests
    line_id = str(line1.iloc[0].line_id)
    line_sn = line1.iloc[0].line_sn

    #The keys for the dataframe that is going to be built
    keys = ['bus','line','direction','stop','isHead','destination','deviation','estimateArrive','DistanceBus']

    #List with all the stops in both directions
    stop_codes = list(set(line_stops_dict[line_id]['1'] + line_stops_dict[line_id]['2']))

    #Function to perform the requests asynchronously, performing them concurrently would be too slow
    async def get_data_asynchronous() :
        row_list = []
        calc_lats,calc_lons = [],[]
        given_lats,given_lons = [],[]

        #We set the number of workers that is going to take care about the requests
        with ThreadPoolExecutor(max_workers=10) as executor:
            #We create a loop object
            loop = asyncio.get_event_loop()
            #And a list of tasks to be performed by the loop
            tasks = [
                loop.run_in_executor(
                    executor,
                    get_arrival_times, #Function that is gonna be called by the tasks
                    *(line_sn,stop_id,accessToken)  #Parameters for the function
                )
                for stop_id in stop_codes
            ]
            #And finally we perform the tasks and gather the information returned by them into two lists
            for response in await asyncio.gather(*tasks) :
                if not response == 'Error' :
                    arrival_data = response.json()
                    if arrival_data['code'] == '98':
                        #Return the data gathered if the request that fails isnt the first
                        return 'Hits of account spent'
                else :
                    continue

                #We get the buses data and stop coords
                buses_data = arrival_data['data'][0]['Arrive']
                stop = stops.loc[stops.id == int(buses_data[0]['stop'])]
                for bus in buses_data :
                    #Given coordinates provided by the API
                    given_lons.append(bus['geometry']['coordinates'][0])
                    given_lats.append(bus['geometry']['coordinates'][1])
                    #We calculate the bus position depending on the direction it belongs to
                    if bus['stop'] in line_stops_dict[line_id]['1'][1:] :
                        bus['direction'] = '1'
                        calc_lon,calc_lat = point_by_distance_on_line(line1,bus['DistanceBus'],stop.lat,stop.lon)
                        calc_lons.append(calc_lon)
                        calc_lats.append(calc_lat)
                    else :
                        bus['direction'] = '2'
                        calc_lon,calc_lat = point_by_distance_on_line(line2,bus['DistanceBus'],stop.lat,stop.lon)
                        calc_lons.append(calc_lon)
                        calc_lats.append(calc_lat)

                    values = [bus[key] for key in keys]
                    row_list.append(dict(zip(keys, values)))

        return row_list, calc_lats, calc_lons, given_lats, given_lons

    #We declare the loop and call it, then we run it until it is complete
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(get_data_asynchronous())
    loop.run_until_complete(future)

    #And once it is completed we gather the information returned by it like this
    future_result = future.result()
    if future_result == 'Hits of account spent' :
        return 'Hits of account spent'
    else :
        row_list = future_result[0]
        calc_lats = future_result[1]
        calc_lons = future_result[2]
        given_lats = future_result[3]
        given_lons = future_result[4]

        #We create the dataframe of the buses
        buses_df = pd.DataFrame(row_list, columns=keys)
        buses_list = [str(row.bus)+'-('+str(row.stop)+')' for row in buses_df.itertuples()]
        final_lats,final_lons = [],[]
        #Use the given coords if they exist
        for i in range(len(calc_lats)) :
            if (given_lats[i]!=0)&(given_lons[i]!=0) :
                final_lats.append(given_lats[i])
                final_lons.append(given_lons[i])
            else :
                final_lats.append(calc_lats[i])
                final_lons.append(calc_lons[i])

        buses_df['lat'] = final_lats
        buses_df['lon'] = final_lons

        buses_df['calc_lat'] = calc_lats
        buses_df['calc_lon'] = calc_lons

        #And then we get only the rows where each bus is closer to a stop (lower DistanceBus attrib)
        frames = []
        final_rows = []
        for bus_id in buses_df.bus.unique() :
            buses_df_reduced = buses_df.loc[(buses_df.bus==bus_id)]
            final_rows.append(buses_df_reduced.loc[buses_df_reduced.DistanceBus==buses_df_reduced.DistanceBus.min()].iloc[0])

        buses_df_unique = pd.DataFrame(final_rows, columns=keys+['lat','lon','calc_lat','calc_lon'])

        #Finally we return all the data
        return buses_df_unique, calc_lats, calc_lons, given_lats, given_lons, buses_list

# CALLBACKS

# CALLBACK 1 - Live Graph of Line Buses
@app.callback(Output(component_id = 'live-update-graph',component_property = 'children'),
              [Input(component_id = 'lineId-select',component_property = 'value'),
              Input(component_id = 'interval-component',component_property = 'n_intervals')])
def update_graph_live(lineId_value,n_intervals):
    '''
    Function that updates the graph each x seconds depending on the value of lineId

        Parameters
        ---
        input_lineId_value: string
            The line whose buses are going to be ploted
    '''
    try:
        lineId = int(lineId_value)

        #And the line rows
        line1 = lines_shapes.loc[(lines_shapes['line_id']==lineId)&(lines_shapes['direction']==1)]
        line2 = lines_shapes.loc[(lines_shapes['line_id']==lineId)&(lines_shapes['direction']==2)]
        #Set the center of the graph to the centroid of the line
        center_x = line1.lon.mean()
        center_y = line2.lat.mean()

        #We obtain the arrival data for the line
        arrival_time_data_complete = get_arrival_time_data_of_line(line1,line2,accessToken)
        if arrival_time_data_complete == 'Hits of account spent' :
            return 'Hits of account spent'
        else :
            arrival_time_data = arrival_time_data_complete[0]
            calc_lats = arrival_time_data_complete[1]
            calc_lons = arrival_time_data_complete[2]
            given_lats = arrival_time_data_complete[3]
            given_lons = arrival_time_data_complete[4]
            buses_list = arrival_time_data_complete[5]

        #We calculate the distance between given and calc coords
        dist_list = []
        for i in range(0,len(calc_lats)) :
            lon1 = calc_lons[i]
            lat1 = calc_lats[i]
            lon2 = given_lons[i]
            lat2 = given_lats[i]
            if (lon2 == 0)&(lat2 == 0) :
                distance = 0
            else :
                distance = haversine((lat1,lon1),(lat2, lon2))
            dist_list.append(distance) #Distancia en metros

        #Style depending on hour
        style = style_day

        #We create the figure object
        fig = go.Figure()
        #Add the bus points to the figure
        for bus in arrival_time_data.itertuples() :
            if bus.direction == '1' :
                color = 'blue'
            else :
                color = 'red'
            fig.add_trace(go.Scattermapbox(
                lat=[bus.lat],
                lon=[bus.lon],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=10,
                    color=color,
                    opacity=0.7
                ),
                text=['{}-{}'.format(bus.bus,bus.stop)],
                hoverinfo='text'
            ))
            #Add the calculated coords points to the figure
            fig.add_trace(go.Scattermapbox(
                lat=[bus.calc_lat],
                lon=[bus.calc_lon],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=8,
                    color='black',
                    opacity=0.7
                ),
                text=['{}-{}'.format(bus.bus,bus.bus)],
                hoverinfo='text'
            ))

        #Add the stops to the figure
        stops_selected1 = stops.loc[stops.id.isin(line_stops_dict[str(lineId)]['1'][1:])]
        fig.add_trace(go.Scattermapbox(
            lat=stops_selected1.lat,
            lon=stops_selected1.lon,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=7,
                color='rgb(108, 173, 245)',
                opacity=0.7
            ),
            text=stops_selected1.id,
            hoverinfo='text'
        ))
        stops_selected2 = stops.loc[stops.id.isin(line_stops_dict[str(lineId)]['2'][1:])]
        fig.add_trace(go.Scattermapbox(
            lat=stops_selected2.lat,
            lon=stops_selected2.lon,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=7,
                color='rgb(243, 109, 90)',
                opacity=0.7
            ),
            text=stops_selected2.id,
            hoverinfo='text'
        ))
        #Add lines to the figure
        for line in [line1,line2] :
            color = 'rgb(108, 173, 245)' if line.iloc[0].direction == 1 else 'rgb(243, 109, 90)'
            fig.add_trace(go.Scattermapbox(
                lat=line.lat,
                lon=line.lon,
                mode='lines',
                line=dict(width=1.5, color=color),
                text='Línea : {}-{}'.format(lineId,line.iloc[0].direction),
                hoverinfo='text'
            ))

        #And set the figure layout
        fig.update_layout(
            title='REAL TIME POSITION OF THE BUSES OF LINE {}'.format(lineId),
            height=500,
            margin=dict(r=0, l=0, t=0, b=0),
            hovermode='closest',
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=dict(
                    lat=center_y,
                    lon=center_x
                ),
                pitch=0,
                zoom=12.5,
                style=style
            )
        )
        #Clear null values (coords not given)
        pop_list = []
        for i in range(0,len(buses_list)) :
            if dist_list[i]==0 :
                pop_list.append(i)
        for index in sorted(pop_list, reverse=True):
            del buses_list[index]
            del dist_list[index]

        #We create the figure object
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=buses_list,
            y=dist_list,
            name='Error bar graph',
            marker_color='#2ca02c'
        ))
        fig2.update_layout(
            height=500,
            margin=dict(r=0, l=0, t=0, b=0),
            yaxis=dict(
                title='Distance(meters) between calculated distance and given one'
            )
        )

        #And finally we return the graph element
        return [
            html.Div(className='columns',children=[
                html.Div(className='column is-two-thirds',children=[
                    html.H2(
                        'Live position of line {} buses'.format(lineId),
                        className = 'subtitle is-4'
                    ),
                    dcc.Graph(
                        id = 'graph',
                        figure = fig
                    )
                ]),
                html.Div(className='column',children=[
                    html.H2(
                        'Error in calculated position:',
                        className = 'subtitle is-4'
                    ),
                    dcc.Graph(
                        id = 'graph2',
                        figure = fig2
                    )
                ])
            ]),
        ]
    except ValueError :
        return 'No active buses were found in the desired line - Maybe the line is not active at the moment'
    except :
        #If there is an error we ask for a valid line id
        return 'Please select a lineId from the list'
