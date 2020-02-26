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
import math
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
        html.Div(className='box',id='live-update-graph'),
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
        'Text_StopRequired_YN': 'Y',
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
    #For night buses
    if len(lineId)==3 and lineId[0]=='5' :
        if lineId[1]=='0' :
            lineId = 'N'+lineId[2]
        else :
            lineId = 'N'+lineId[1:]
    #Buses with letter id
    elif lineId == '91' :
        lineId = 'F'
    elif lineId == '92' :
        lineId = 'G'
    elif lineId == '68' :
        lineId = 'C1'
    elif lineId == '69' :
        lineId = 'C2'
    
    #We get the LineString object and length from the rows
    line1_geom = line1['geometry']
    line1_length = line1['dist']
    line2_geom = line2['geometry']
    line2_length = line2['dist']
    
    #The keys for the dataframe that is going to be built
    keys = ['bus','line','direction','stop','isHead','destination','deviation','estimateArrive','DistanceBus']

    #List with all the stops in both directions
    stop_codes = stops_dir1 + stops_dir2
    
    #Function to perform the requests asynchronously, performing them concurrently would be too slow
    async def get_data_asynchronous() :
        row_list = []
        points_list = []
        real_coords_list = []
        
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
            #And finally we perform the tasks and gather the information returned by them into two lists
            for response in await asyncio.gather(*tasks) :
                if not response == 'Error' :
                    arrival_data = response.json()
                    if arrival_data['code'] == '98':
                        #Return the data gathered if the request that fails isnt the first
                        if len(row_list) == 0 :
                            return 'Hits of account spent'
                        else :
                            return [row_list,points_list,real_coords_list]
                else :
                    continue

                #We get the buses data and stop coords
                buses_data = arrival_data['data'][0]['Arrive']
                stop_coords = Point(arrival_data['data'][0]['StopInfo'][0]['geometry']['coordinates'])
                for bus in buses_data :
                    #Real coordinates provided by the API
                    real_coords_list.append(Point(bus['geometry']['coordinates']))
                    #We calculate the bus position depending on the direction it belongs to
                    if bus['stop'] in stops_dir1 :
                        bus['direction'] = '1'
                        points_list.append(Point(point_by_distance_on_line(line1_geom,line1_length,bus['DistanceBus']/1000,stop_coords)))
                    else :
                        bus['direction'] = '2'
                        points_list.append(Point(point_by_distance_on_line(line2_geom,line2_length,bus['DistanceBus']/1000,stop_coords)))
                    values = [bus[key] for key in keys]
                    row_list.append(dict(zip(keys, values)))
                    
        return [row_list,points_list,real_coords_list]
    
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
        points_list = future_result[1]
        real_coords_list = future_result[2]
    
        #We create the dataframe of the buses
        buses_gdf = pd.DataFrame(row_list, columns=keys)
        buses_list = []
        for index,row in buses_gdf.iterrows() :
            buses_list.append(str(row['bus'])+'-('+str(row['stop'])+')')
        final_coords_list = []
        #Use the real coords if they exist
        for i in range(0,len(points_list)) :
            if (real_coords_list[i].y!=0)&(real_coords_list[i].x!=0) :
                final_coords_list.append(real_coords_list[i])
            else :
                final_coords_list.append(points_list[i])
        buses_gdf['geometry'] = final_coords_list
        buses_gdf['calc_coord'] = points_list

        #And then we get only the rows where each bus is closer to a stop (lower DistanceBus attrib)
        frames = []
        for busId in buses_gdf['bus'].unique() :
            buses_gdf_reduced = buses_gdf.loc[(buses_gdf['bus']==busId)]
            frames.append(buses_gdf_reduced.loc[buses_gdf_reduced['DistanceBus']==buses_gdf_reduced['DistanceBus'].min()])

        buses_gdf_unique = pd.concat(frames)
        
        #Finally we return the geodataframe
        return [gpd.GeoDataFrame(buses_gdf_unique,crs=fiona.crs.from_epsg(4326),geometry='geometry'),points_list,real_coords_list,buses_list]

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
        arrival_time_data_complete = get_arrival_time_data_of_line(lineId,line1,line2,stops_dir1,stops_dir2,accessToken)
        if arrival_time_data_complete == 'Hits of account spent' :
            return 'Hits of account spent'
        else :
            arrival_time_data = arrival_time_data_complete[0]
            points_list = arrival_time_data_complete[1]
            real_coords_list = arrival_time_data_complete[2]
            buses_list = arrival_time_data_complete[3]
        
        #We calculate the distance between real and calc coords
        dist_list = []
        for i in range(0,len(points_list)) :
            lon1 = points_list[i].x
            lat1 = points_list[i].y
            lon2 = real_coords_list[i].x
            lat2 = real_coords_list[i].y
            if (lon2 == 0)&(lat2 == 0) :
                distance = 0
            else :
                distance = haversine((lat1,lon1),(lat2, lon2))
            dist_list.append(distance) #Distancia en metros
            
        #Style depending on hour
        now = datetime.datetime.now()
        if (datetime.time(6,0,0) <= now.time() <= datetime.time(23,30,0)) :
            style = style_day
        else :
            style = style_night
            
        #We create the figure object
        fig = go.Figure()
        #Add the bus points to the figure
        for index,bus in arrival_time_data.iterrows() :
            if bus['direction'] == '1' :
                color = 'blue'
            else : 
                color = 'red'
            fig.add_trace(go.Scattermapbox(
                lat=[bus['geometry'].y],
                lon=[bus['geometry'].x],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=10,
                    color=color,
                    opacity=0.7
                ),
                text=['{}-{}'.format(bus['bus'],bus['stop'])],
                hoverinfo='text'
            ))
            #Add the calculated coords points to the figure
            fig.add_trace(go.Scattermapbox(
                lat=[bus['calc_coord'].y],
                lon=[bus['calc_coord'].x],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=8,
                    color='purple',
                    opacity=0.7
                ),
                text=['{}-{}'.format(bus['bus'],bus['stop'])],
                hoverinfo='text'
            ))
            
        
        stops_of_lines = list(set(stops_dir1 + stops_dir2))
        stops_selected = stops.loc[stops['stop_code'].isin(stops_of_lines)]
        #Add the stops to the figure
        fig.add_trace(go.Scattermapbox(
            lat=stops_selected['geometry'].y,
            lon=stops_selected['geometry'].x,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=5,
                color='green',
                opacity=0.5
            ),
            text=stops_selected['stop_code'],
            hoverinfo='text'
        ))
        
        #Plot the lines in soft color
        for index, row in route_lines.loc[route_lines['line_id']==lineId].iterrows():
            line = row['geometry']
            x_coords = []
            y_coords = []
            for coords in list(line.coords) :
                x_coords.append(coords[0])
                y_coords.append(coords[1])
            
            if row['direction'] == '1':
                color = 'rgb(108, 173, 245)'
            else :
                color = 'rgb(243, 109, 90)'
                
            fig.add_trace(go.Scattermapbox(
                lat=y_coords,
                lon=x_coords,
                mode='lines',
                line=dict(width=1, color=color),
                text='Línea : {}-{}'.format(row['line_id'],row['direction']),
                hoverinfo='text'
            ))
        #And set the figure layout
        fig.update_layout(
            title='REAL TIME POSITION OF THE BUSES OF LINE {}'.format(lineId),
            height=600,
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
        fig.update_layout(
            height=600,
            margin=dict(r=0, l=0, t=0, b=0),
            yaxis=dict(
                title='Distance(meters)'
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
                    html.H2(
                        'distance(RealCoords,CalcCoords) in meters for pair "bus-(stop)"',
                        className = 'subtitle is-5'
                    ),
                    dcc.Graph(
                        id = 'graph2',
                        figure = fig2
                    )
                ])
            ]),
        ]
