import dash_core_components as dcc
import dash_html_components as html
import dash_table
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

import datetime 
from datetime import timedelta

from app import app

# WE LOAD THE DATA
stops = gpd.read_file('M6Data/stops.json')
route_lines = gpd.read_file('M6Data/route_lines.json')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)

    
#Load the buses dataframe and parse the dates
buses_data = pd.read_csv('../../flash/EMTBuses/buses_data.csv')
buses_data['datetime'] = pd.to_datetime(buses_data['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
start_date = buses_data.iloc[0]['datetime']
minutes_range = (datetime.datetime.now() - start_date).total_seconds() / 60.0
estimateArrive_range = buses_data['estimateArrive'].max()
DistanceBus_range = buses_data['DistanceBus'].max()
lines_retrieved = ['1','82','F','G','U','132','N2','N6']
buses_retrieved = buses_data['bus'].unique().tolist()
stops_retrieved = buses_data['stop'].unique().tolist()

layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('DATA RETRIEVED MONITORING',className = 'title is-3'),
        html.Span('Time Interval: ', className = 'tag is-light is-medium'),
        dcc.RangeSlider(
            id='time-range-slider',
            marks = {
                i*minutes_range/20: {
                    'label' : (start_date + timedelta(minutes=i*minutes_range/20)).strftime("%d-%m-%Y (%H:%M)"), 
                    'style' : {'font-size': '8px'}} 
                for i in range(1, 20)
            },
            min=0,
            max=minutes_range,
            step=1,
            value=[minutes_range-120, minutes_range]
        ),
        html.Div(className='columns',children = [
            html.Div(className='column is-one-third',children = [
                html.Span('Select lines: ', className = 'tag is-light is-medium'),
                dcc.Dropdown(
                    id="lines-select",
                    options=[{"label": i, "value": i} for i in lines_retrieved + ['All']],
                    value='All',
                    searchable=True,
                    multi=True
                )
            ]),
            html.Div(className='column is-one-third',children = [
                html.Span('Select stops: ', className = 'tag is-light is-medium'),
                dcc.Dropdown(
                    id="stops-select",
                    options=[{"label": i, "value": i} for i in stops_retrieved + ['All']],
                    value='All',
                    searchable=True,
                    multi=True
                )
            ]),
            html.Div(className='column is-one-third',children = [
                html.Span('Select buses: ', className = 'tag is-light is-medium'),
                dcc.Dropdown(
                    id="buses-select",
                    options=[{"label": i, "value": i} for i in buses_retrieved + ['All']],
                    value='All',
                    searchable=True,
                    multi=True
                )
            ])   
        ]),
        html.Div(className='columns',children = [
            html.Div(className='column is-half',children = [
                html.Span('DistanceBus Range: ', className = 'tag is-light is-medium'),
                dcc.RangeSlider(
                    id='distance-range-slider',
                    marks = {
                        i*DistanceBus_range/10: {
                            'label' : '{}(km)'.format(round(i*(DistanceBus_range/10000),1)), 
                            'style' : {'font-size': '10px'}
                        } for i in range(0, 10)
                    },
                    min=-1,
                    max=DistanceBus_range,
                    step=100,
                    value=[0, DistanceBus_range]
                )
            ]),
            html.Div(className='column is-half',children = [
                html.Span('ETA Range: ', className = 'tag is-light is-medium'),
                dcc.RangeSlider(
                    id='eta-range-slider',
                    marks = {
                        i*estimateArrive_range/10: {
                            'label' : '{}(min)'.format(round(i*(estimateArrive_range/600),1)), 
                            'style' : {'font-size': '10px'}
                        } for i in range(0, 10)
                    },
                    min=0,
                    max=estimateArrive_range,
                    step=1,
                    value=[0, estimateArrive_range]
                )
            ]) 
        ]),
        html.Div(className='box',id='live-update-data')
    ])
    
])

# CALLBACKS

# CALLBACK 1 - Retrieved data
@app.callback([
        Output(component_id = 'live-update-data',component_property = 'children')
    ],
    [
        Input('time-range-slider', 'value'),
        Input('lines-select', 'value'),
        Input('stops-select', 'value'),
        Input('buses-select', 'value')
    ])

def update_graph_live(time_range,lines_selected,stops_selected,buses_selected):
            
        try :
            showAllLines = False
            showAllStops = False
            showAllBuses = False

            #We add the minutes and get the start and end of the time interval
            start_interval = start_date + timedelta(minutes=time_range[0])
            end_interval = start_date + timedelta(minutes=time_range[1])

            if type(lines_selected) is list:
                if 'All' in lines_selected :
                    showAllLines = True
            else :
                if lines_selected == 'All' :
                    showAllLines = True
                    
            if type(stops_selected) is list:
                if 'All' in stops_selected :
                    showAllStops = True
            else :
                if buses_selected == 'All' :
                    showAllStops = True

            if type(buses_selected) is list:
                if 'All' in buses_selected :
                    showAllBuses = True
            else :
                if buses_selected == 'All' :
                    showAllBuses = True

            #Get the rows that are inside the time interval
            mask = (buses_data['datetime'] > start_interval) & (buses_data['datetime'] < end_interval)
            buses_data_reduced = buses_data.loc[mask]

            #Get the rows with the lines selected
            if not showAllLines :
                if type(lines_selected) is list:
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['line'].isin(lines_selected)]
                else :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['line']==lines_selected]
            
            #Get the rows with the stops selected
            if not showAllStops :
                if type(stops_selected) is list:
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['stop'].isin(stops_selected)]
                else :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['stop']==stops_selected]
                    
            #Get the rows with the buses selected
            if not showAllBuses :
                if type(buses_selected) is list:
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['bus'].isin(buses_selected)]
                else :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['bus']==buses_selected]

            #And finally we return the graph element
            return [
                html.Div(className='',children=[
                    html.Div(className='',children=[
                        dcc.Loading(type = 'graph', children = [
                            html.H2(
                                'Showing data from: {} to: {}'.format(start_interval.strftime("%d-%m-%Y (%H:%M)"),end_interval.strftime("%d-%m-%Y (%H:%M)")),
                                className = 'subtitle is-4'
                            ),
                            dash_table.DataTable(
                                id='table',
                                columns=[{"name": i, "id": i} for i in buses_data_reduced.columns],
                                data=buses_data_reduced.to_dict('records'),
                                page_size= 15,
                                style_table={'overflowX': 'scroll'},
                                style_cell={
                                    'minWidth': '0px', 'maxWidth': '180px',
                                    'overflow': 'hidden',
                                    'textOverflow': 'ellipsis',
                                }
                            )
                        ])
                    ])
                ])
            ]
        
        except : 
            return [
                'No rows found for the selection'
            ]
        