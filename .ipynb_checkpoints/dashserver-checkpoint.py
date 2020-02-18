import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import plotly.graph_objects as go

import requests

import pandas as pd
import geopandas as gpd

from shapely.geometry import shape
from shapely.geometry import Point, LineString
from shapely import wkt
import fiona

import asyncio
from concurrent.futures import ThreadPoolExecutor

import json

#We setup the app
app = dash.Dash(__name__)

#We import the layout from another file
from html_layout import index_string
app.index_string = index_string

#SERVER LAYOUT FOR THE INTERACTIVE ELEMENTS
app.layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('Mapa de lineas y paradas EMT',className = 'title is-3'),
        html.Iframe(id='map',srcDoc=open('M6Data/routes_stops.html','r').read(),width='100%',height='600')
    ]),

    html.Div(className = 'box', children = [
        html.H1('Seguimiento de ubicación de buses de una línea deseada',className = 'title is-3'),
        html.Span('Line ID: ', className = 'tag is-light is-large'),
        dcc.Input(className = "input is-primary", placeholder = 'Introduce linea deseada', id = 'input_lineId', value = '', type = 'text'),
        html.Div(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=3*1000, # in milliseconds
            n_intervals=0
        )
    ])



])

# CARGAMOS LOS DATOS
stops = gpd.read_file('M6Data/stops.json')
route_lines = gpd.read_file('M6Data/route_lines.json')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)

# API FUNCTIONS
def get_access_token(email,password) :

    response = requests.get(
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
    from shapely.geometry import shape

    response = requests.get(
        'https://openapi.emtmadrid.es/v2/transport/busemtmad/lines/{}/stops/{}/'.format(lineId,direction),
        headers = {'accessToken': accessToken},
        timeout = 5
    )

    stops_data = pd.DataFrame(response.json()['data'][0]['stops'])
    stops_data['geometry'] = [shape(i) for i in stops_data['geometry']]
    stops_data = gpd.GeoDataFrame(stops_data).set_geometry('geometry')
    line_stops = stops_data[['stop','name','postalAddress','pmv','dataLine','geometry']]
    return line_stops

def get_arrival_times(lineId,stopId,accessToken) :
    body = {
        'cultureInfo': 'ES',
        'Text_StopRequired_YN': 'Y',
        'Text_EstimationsRequired_YN': 'Y',
        'Text_IncidencesRequired_YN': 'N',
        'DateTime_Referenced_Incidencies_YYYYMMDD':'20200130'
    }
    response = requests.post(
        'https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{}/arrives/{}/'.format(stopId,lineId),
        data = json.dumps(body),
        headers = {
            'accessToken': accessToken,
            'Content-Type': 'application/json'
        },
        timeout = 5
    )
    response_json = response.json()
    arrival_data = response_json['data'][0]['Arrive']
    stop_coords = Point(response_json['data'][0]['StopInfo'][0]['geometry']['coordinates'])
    return [arrival_data,stop_coords]

# FUNCTIONS
def point_by_distance_on_line (line, line_lenght, distance, origin_point) :
    normalized_distance = line.project(origin_point,normalized=True) - distance/line_lenght
    interpolated_point = line.interpolate(normalized_distance,normalized=True)
    return Point(interpolated_point.x,interpolated_point.y)

def get_arrival_time_data_of_line(lineId,line1,line2,stops_dir1,stops_dir2,accessToken) :
    line1_geom = line1['geometry']
    line1_length = line1['dist']
    line2_geom = line2['geometry']
    line2_length = line2['dist']

    keys = ['bus','line','stop','isHead','destination','deviation','estimateArrive','DistanceBus']

    stop_codes = stops_dir1 + stops_dir2

    async def get_data_asynchronous() :
        row_list = []
        points_list = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    get_arrival_times,
                    *(lineId,stopId,accessToken)
                )
                for stopId in stop_codes
            ]
            for arrival_data in await asyncio.gather(*tasks) :
                arrival_times = arrival_data[0]
                stop_coords = arrival_data[1]
                for bus in arrival_times :
                    points_list.append(point_by_distance_on_line(line1_geom,line1_length,bus['DistanceBus']/1000,stop_coords))
                    values = [
                        bus['bus'],
                        bus['line'],
                        bus['stop'],
                        bus['isHead'],
                        bus['destination'],
                        bus['deviation'],
                        bus['estimateArrive'],
                        bus['DistanceBus']
                     ]
                    row_list.append(dict(zip(keys, values)))
        return [row_list,points_list]
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(get_data_asynchronous())
    loop.run_until_complete(future)

    row_list = future.result()[0]
    points_list = future.result()[1]

    buses_gdf = pd.DataFrame(row_list, columns=keys)
    buses_gdf['geometry'] = points_list

    frames = []
    for busId in buses_gdf['bus'].unique() :
        buses_gdf_reduced = buses_gdf.loc[(buses_gdf['bus']==busId)]
        frames.append(buses_gdf_reduced.loc[buses_gdf_reduced['DistanceBus']==buses_gdf_reduced['DistanceBus'].min()])

    buses_gdf_unique = pd.concat(frames)

    return gpd.GeoDataFrame(buses_gdf_unique,crs=fiona.crs.from_epsg(4326),geometry='geometry')

# INICIAMOS SESION EN LA API DE LA EMT
from api_credentials import email,password
accessToken = get_access_token(email,password)

# CALLBACKS

# CALLBACK 1 - Live Graph of Line
# Multiple components can update everytime interval gets fired.
@app.callback(Output(component_id = 'live-update-graph',component_property = 'children'),
              [Input(component_id = 'input_lineId',component_property = 'value'),
              Input(component_id = 'interval-component',component_property = 'n_intervals')])
def update_graph_live(input_lineId_value,n_intervals):
    try:
        lineId = input_lineId_value

        # Obtenemos los datos de las paradas pertenecientes a la linea de interes
        stops_dir1 = line_stops_dict[lineId]['1']['stops']
        stops_dir2 = line_stops_dict[lineId]['2']['stops']
        
        line1 = route_lines.loc[(route_lines['line_id']==lineId)&(route_lines['direction']=='1')]
        line2 = route_lines.loc[(route_lines['line_id']==lineId)&(route_lines['direction']=='2')]
        center = line1['geometry'].centroid
        center_x = float(center.x)
        center_y = float(center.y)
        
        
        # Obtenemos datos de llegada de esa línea
        arrival_time_data = get_arrival_time_data_of_line(lineId,line1,line2,stops_dir1,stops_dir2,accessToken)

        mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
        
        fig = go.Figure()
        
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
        
        fig.update_layout(
            title='Posición en tiempo real de los buses de la línea {}'.format(lineId),
            autosize=True,
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
                zoom=12,
                style='mapbox://styles/alejp1998/ck6qp34qz52ul1ipfio20oe2w'
            )
        )
        
        return dcc.Graph(
            id = 'graph',
            figure = fig
        )
    except:
        return 'Por favor introduce un ID de línea válido'

#START THE SERVER
if __name__ == '__main__' :
    app.run_server(debug=True)
