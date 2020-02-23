import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
import geopandas as gpd
import json

import plotly.graph_objects as go
from shapely.geometry import shape
from shapely.geometry import Point, LineString
from shapely import wkt
import fiona

import asyncio
from concurrent.futures import ThreadPoolExecutor

import datetime

from app import app

import random

# CARGAMOS LOS DATOS
stops = gpd.read_file('M6Data/stops.json')
route_lines = gpd.read_file('M6Data/route_lines.json')
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
        html.Div(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        )
    ])
    
])

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
    response = requests_retry_session().get(
        'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
        headers={
            'email':email,
            'password':password
        },
        timeout=5
    )

    json_response = response.json()
    accessToken = json_response['data'][0]['accessToken']
    return accessToken

def get_stops_of_line(lineId,direction,accessToken) :
    """
    Returns the list of stops for the line and direction desired

        Parameters
        ----------
        lineId : string
            The line id
        direction : string
            The direction (1 or 2)
        accessToken: string
            The accessToken obtained in the login
    """
    
    from shapely.geometry import shape

    response = requests_retry_session().get(
        'https://openapi.emtmadrid.es/v2/transport/busemtmad/lines/{}/stops/{}/'.format(lineId,direction),
        headers = {'accessToken': accessToken},
        timeout = 5
    )
    
    #We turn the data of the stops from the response into a dataframe
    stops_data = pd.DataFrame(response.json()['data'][0]['stops'])   
    #And transform the geometry coordinates into point objects
    stops_data['geometry'] = [shape(i) for i in stops_data['geometry']]
    return stops_data

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
        'Text_StopRequired_YN': 'Y',
        'Text_EstimationsRequired_YN': 'Y',
        'Text_IncidencesRequired_YN': 'N',
        'DateTime_Referenced_Incidencies_YYYYMMDD':'20200130'
    }
    
    #And we perform the request
    response = requests_retry_session().post(
        'https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{}/arrives/{}/'.format(stopId,lineId),
        data = json.dumps(body),
        headers = {
            'accessToken': accessToken,
            'Content-Type': 'application/json'
        },
        timeout = 5
    )
    
    #We turn the data into a json and return the arrival data and the coordinates of the stop that made the call
    response_json = response.json()
    arrival_data = response_json['data'][0]['Arrive']
    stop_coords = Point(response_json['data'][0]['StopInfo'][0]['geometry']['coordinates'])
    return [arrival_data,stopId,stop_coords]

# FUNCTIONS
def point_by_distance_on_line (line, line_lenght, distance, origin_point) :
    """
    Returns the coordinates of the bus location

        Parameters
        ----------
        line : LineString
            The line that the bus belongs to
        line_length : float
            The length of the line
        distance : float
            The distance of the bus to the stop in kilometers
        origin_point : Point
            The location of the bus stop
    """
    
    #First we calculate the normalized distance of the bus from the start of the line
    #by substracting the distance of the bus to the stop to the distance of the stop to the start of the line
    #which is returned by the project method of the shapely module
    normalized_distance = line.project(origin_point,normalized=True) - distance/line_lenght
    
    #Then we get the the coordinates of the point that is at the normalized distance obtained 
    #before from the start of the line with the interpolate method
    interpolated_point = line.interpolate(normalized_distance,normalized=True)
    
    #And we return the coordinates of the point
    return (interpolated_point.x,interpolated_point.y)

def get_arrival_time_data_of_line(lineId,line1,line2,stops_dir1,stops_dir2,accessToken) :
    """
    Returns the data of all the buses inside the desired line

        Parameters
        ----------
        lineId : string
            The line id
        line1 : DataFrame row
            Data about the line in direction 1
        line2 : DataFrame row
            Data about the line in direction 2
        stops_dir1 : list
            The list of the stops for direction 1
        stops_dir2 : list
            The list of the stops for direction 2
        accessToken: string
            The accessToken obtained in the login
    """
    #We get the LineString object and length from the rows
    line1_geom = line1['geometry']
    line1_length = line1['dist']
    line2_geom = line2['geometry']
    line2_length = line2['dist']
    
    #The keys for the dataframe that is going to be built
    keys = ['bus','line','stop','isHead','destination','deviation','estimateArrive','DistanceBus']

    #List with all the stops in both directions
    stop_codes = stops_dir1 + stops_dir2
    
    #Function to perform the requests asynchronously, performing them concurrently would be too slow
    async def get_data_asynchronous() :
        row_list = []
        points_list = []
        
        #We set the number of workers that is going to take care about the requests
        with ThreadPoolExecutor(max_workers=10) as executor:
            #We create a loop object
            loop = asyncio.get_event_loop()
            #And a list of tasks to be performed by the loop
            tasks = [
                loop.run_in_executor(
                    executor,
                    get_arrival_times, #Function that is gonna be called by the tasks
                    *(lineId,stopId,accessToken)  #Parameters for the function
                )
                for stopId in stop_codes
            ]
            #We select just half of the tasks to perform randomly
            #tasks = random.sample(tasks, len(tasks)/2)
            #And finally we perform the tasks and gather the information returned by them into two lists
            for arrival_data in await asyncio.gather(*tasks) :
                arrival_times = arrival_data[0]
                stop_id = arrival_data[1]
                stop_coords = arrival_data[2]
                for bus in arrival_times :
                    values = [bus[key] for key in keys]
                    row_list.append(dict(zip(keys, values)))
                    #We calculate the bus position depending on the direction it belongs to
                    if stop_id in stops_dir1 :
                        points_list.append(Point(point_by_distance_on_line(line1_geom,line1_length,bus['DistanceBus']/1000,stop_coords)))
                    else :
                        points_list.append(Point(point_by_distance_on_line(line2_geom,line2_length,bus['DistanceBus']/1000,stop_coords)))
        return [row_list,points_list]
    
    #We declare the loop and call it, then we run it until it is complete
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(get_data_asynchronous())
    loop.run_until_complete(future)
    
    #And once it is completed we gather the information returned by it like this
    row_list = future.result()[0]
    points_list = future.result()[1]
    
    #We create the dataframe of the buses
    buses_gdf = pd.DataFrame(row_list, columns=keys)
    buses_gdf['geometry'] = points_list
    
    #And then we get only the rows where each bus is closer to a stop (lower DistanceBus attrib)
    frames = []
    for busId in buses_gdf['bus'].unique() :
        buses_gdf_reduced = buses_gdf.loc[(buses_gdf['bus']==busId)]
        frames.append(buses_gdf_reduced.loc[buses_gdf_reduced['DistanceBus']==buses_gdf_reduced['DistanceBus'].min()])

    buses_gdf_unique = pd.concat(frames)
    
    #Finally we return the geodataframe
    return gpd.GeoDataFrame(buses_gdf_unique,crs=fiona.crs.from_epsg(4326),geometry='geometry')

# WE LOGIN IN THE EMT API
from api_credentials import email,password
accessToken = get_access_token(email,password)
#Token for the mapbox api
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style_day = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
style_night = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'

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
        lineId = lineId_value

        #We get the stops of the line from the dict
        stops_dir1 = line_stops_dict[lineId]['1']['stops']
        stops_dir2 = line_stops_dict[lineId]['2']['stops']
        #And the line rows
        line1 = route_lines.loc[(route_lines['line_id']==lineId)&(route_lines['direction']=='1')]
        line2 = route_lines.loc[(route_lines['line_id']==lineId)&(route_lines['direction']=='2')]
        #Set the center of the graph to the centroid of the line
        center = line1['geometry'].centroid
        center_x = float(center.x)
        center_y = float(center.y)
        
        #We obtain the arrival data for the line
        arrival_time_data = get_arrival_time_data_of_line(lineId,line1,line2,stops_dir1,stops_dir2,accessToken)
        
        #Style depending on hour
        now = datetime.datetime.now()
        if (datetime.time(hour=6, minute=0) <= now.time() <= datetime.time(hour=23, minute=30)) :
            style = style_day
        else :
            style = style_night
            
        #We create the figure object
        fig = go.Figure()
        #Add the traces to the figure
        fig.add_trace(go.Scattermapbox(
            lat=arrival_time_data['geometry'].y,
            lon=arrival_time_data['geometry'].x,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=10,
                color='rgb(255, 0, 0)',
                opacity=0.7
            ),
            text=arrival_time_data['bus'],
            hoverinfo='text'
        ))
        #And set the figure layout
        fig.update_layout(
            title='REAL TIME POSITION OF THE BUSES OF LINE {}'.format(lineId),
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
                zoom=13,
                style=style
            )
        )
        #And finally we return the graph element
        return dcc.Graph(
            id = 'graph',
            figure = fig
        )
    except:
        #If there is an error we ask for a valid line id
        return 'Please select a lineId from the list'