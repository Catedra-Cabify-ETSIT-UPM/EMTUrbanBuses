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

from app import app

# WE LOAD THE DATA
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
            interval=7.5*1000, # in milliseconds
            n_intervals=0
        )
    ])
    
])

# CALLBACKS

# CALLBACK 1 - Live Graph of Line Buses
@app.callback(Output(component_id = 'live-update-graph',component_property = 'children'),
              [Input(component_id = 'lineId-select',component_property = 'value'),
              Input(component_id = 'interval-component',component_property = 'n_intervals')])
def update_graph_live(lineId_value,n_intervals):

        lineId = lineId_value
        
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