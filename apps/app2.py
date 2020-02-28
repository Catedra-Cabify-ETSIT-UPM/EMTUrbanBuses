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
int_columns = ['stop','bus','real_coords','pos_in_burst','deviation','estimateArrive','DistanceBus','request_time']
for column in int_columns :
    buses_data[column] = pd.to_numeric(buses_data[column])

#Values for components
start_date = buses_data.iloc[0]['datetime']
minutes_range = (datetime.datetime.now() - start_date).total_seconds() / 60.0
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
            html.Div(className='column',children = [
                html.Span('Select stops: ', className = 'tag is-light is-medium'),
                dcc.Dropdown(
                    id="stops-select",
                    options=[{"label": i, "value": i} for i in stops_retrieved + ['All']],
                    value='All',
                    searchable=True,
                    multi=True
                )
            ]),
            html.Div(className='column',children = [
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
            html.Div(className='column is-narrow',children = [
                dcc.RadioItems(
                    id='distance-range-radio',
                    options=[
                        {'label': 'Show All', 'value': 'All'},
                        {'label': 'Use range', 'value': 'Range'},
                    ],
                    value='All',
                    labelStyle={'display': 'block'}
                )  
            ]),
            html.Div(className='column',children = [
                html.Span('DistanceBus Range (km): ', className = 'tag is-light is-medium'),
                dcc.RangeSlider(
                    id='distance-range-slider',
                    marks = {
                        i*200 : {
                            'label' : '{}'.format(i*2/10), 
                            'style' : {'font-size': '10px'}
                        } for i in range(0, 21)
                    },
                    min=0,
                    max=4000,
                    step=1,
                    value=[0, 4000]
                )
            ]),
            html.Div(className='column is-narrow',children = [
                dcc.RadioItems(
                    id='eta-range-radio',
                    options=[
                        {'label': 'Show All', 'value': 'All'},
                        {'label': 'Use range', 'value': 'Range'},
                    ],
                    value='All',
                    labelStyle={'display': 'block'}
                )  
            ]),
            html.Div(className='column',children = [
                html.Span('ETA Range (min): ', className = 'tag is-light is-medium'),
                dcc.RangeSlider(
                    id='eta-range-slider',
                    marks = {
                        i*180 : {
                            'label' : '{}'.format(i*3), 
                            'style' : {'font-size': '10px'}
                        } for i in range(0, 21)
                    },
                    min=0,
                    max=3600,
                    step=1,
                    value=[0, 3600]
                )
            ]) 
        ]),
        html.Div(className='box',id='selected-data'),
        html.Div(className='box',id='figs-selected-data')
    ])
    
])

# CALLBACKS

# CALLBACK 1 - Retrieved data
@app.callback([
        Output(component_id = 'selected-data',component_property = 'children')
    ],
    [
        Input('time-range-slider', 'value'),
        Input('lines-select', 'value'),
        Input('stops-select', 'value'),
        Input('buses-select', 'value'),
        Input('distance-range-radio', 'value'),
        Input('distance-range-slider', 'value'),
        Input('eta-range-radio', 'value'),
        Input('eta-range-slider', 'value')
    ])

def update_graph_live(time_range,lines_selected,stops_selected,buses_selected,distance_radio,distance_range,eta_radio,eta_range):
            
        try :
            showAllLines = False
            showAllStops = False
            showAllBuses = False
            showAllDistances = False
            showAllEtas = False

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
                if stops_selected == 'All' :
                    showAllStops = True

            if type(buses_selected) is list:
                if 'All' in buses_selected :
                    showAllBuses = True
            else :
                if buses_selected == 'All' :
                    showAllBuses = True
                    
            if distance_radio == 'All' :
                showAllDistances = True
            
            if eta_radio == 'All' :
                showAllEtas = True

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
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['stop']==int(stops_selected)]
                    
            #Get the rows with the buses selected
            if not showAllBuses :
                if type(buses_selected) is list:
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['bus'].isin(buses_selected)]
                else :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['bus']==int(buses_selected)]
            
            #Get the rows with the distances selected
            if not showAllDistances :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['DistanceBus'].between(distance_range[0],distance_range[1])]
            
            #Get the rows with the distances selected
            if not showAllEtas :
                    buses_data_reduced = buses_data_reduced.loc[buses_data_reduced['estimateArrive'].between(eta_range[0],eta_range[1])]
                    
            #If no rows suit in the selection
            if (buses_data_reduced.shape[0] == 0) | (buses_data_reduced.shape[0] == 0) :
                return [
                    'No rows found for the selection'
                ]
            
            #And finally we return the graph element
            return [
                html.Div(className='',children=[
                    html.Div(className='',children=[
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
                            },
                            editable=True,
                            filter_action="native",
                            sort_action="native",
                            sort_mode="multi",
                            row_deletable=True
                        )
                    ])
                ])
            ]
        except :
            return [
                'No rows found for the selection'
            ]
        


# CALLBACK 2 - Representations of selected data
@app.callback(
        Output('figs-selected-data', "children"),
    [
        Input('table', "derived_virtual_data")
    ])
def update_graphs(rows):

    if len(rows) < 500 :
        buses_data_reduced = pd.DataFrame(rows)
        buses_data_reduced['datetime'] = pd.to_datetime(buses_data_reduced['datetime'], format='%Y-%m-%dT%H:%M:%S.%f')
        
        #ETAs Figures
        first_stop = buses_data_reduced['stop'].unique().tolist()[0]
        fs_df = buses_data_reduced.loc[buses_data_reduced['stop'] == first_stop]
        fs_buses = fs_df['bus'].unique().tolist()
        
        fig1 = go.Figure() #Distances of bus to stop
        fig2 = go.Figure() #ETAs
        fig3 = go.Figure() #Error in ETAs
        for bus in fs_buses :
            fs_bus_df = fs_df.loc[fs_df['bus']==bus]
            line = fs_bus_df.iloc[0]['line']
            bus_times = fs_bus_df['datetime'].tolist()
            last_time = bus_times[-1]
            bus_dists = [dist/1000 for dist in fs_bus_df['DistanceBus'].tolist()]
            bus_etas = [eta/60 for eta in fs_bus_df['estimateArrive'].tolist()]
            bus_etas_error = []
            if bus_etas[-1] <= 1 : 
                for i in range(len(bus_etas)) :
                    error = (bus_times[i] + timedelta(minutes=bus_etas[i])) - last_time
                    error_mins = error.total_seconds()/60
                    bus_etas_error.append(error_mins)
                
            # Create and style traces
            fig1.add_trace(go.Scatter(x=bus_times, y=bus_dists, name='{}-{}'.format(line,bus),
                                     line=dict(width=4)))
            fig2.add_trace(go.Scatter(x=bus_times, y=bus_etas, name='{}-{}'.format(line,bus),
                                     line=dict(width=4)))
            fig3.add_trace(go.Scatter(x=bus_times, y=bus_etas_error, name='{}-{}'.format(line,bus),
                                     line=dict(width=4)))
        # Edit the layout
        fig1.update_layout(title='Distance remaining for the buses heading stop {}'.format(first_stop),
                           xaxis_title='Time',
                           yaxis_title='DISTANCE (km)')
        # Edit the layout
        fig2.update_layout(title='ETAs for the buses heading stop {}'.format(first_stop),
                           xaxis_title='Time',
                           yaxis_title='ETA (minutes)')
        # Edit the layout
        fig3.update_layout(title='Error in ETAs for the buses heading stop {}. Positive or negative if it arrives later or sooner than expected, respectively'.format(first_stop),
                           xaxis_title='Time',
                           yaxis_title='ETA ERROR (minutes)')
        
        return [
            dcc.Graph(
                id='fig1',
                figure=fig1
            ),
            dcc.Graph(
                id='fig2',
                figure=fig2
            ),
            dcc.Graph(
                id='fig3',
                figure=fig3
            )
        ]
    else :
        return 'Selected data is too large to represent. Select a smaller slice'