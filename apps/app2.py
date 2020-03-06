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

#Token for the mapbox api
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style_day = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
style_night = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
    
#Load the buses dataframe and parse the dates
buses_data = pd.read_csv('../../flash/EMTBuses/buses_data.csv',dtype={'line': 'str','destination': 'str','stop': 'int32','bus': 'int32','given_coords': 'int32','pos_in_burst':'int32','deviation': 'int32','estimateArrive': 'int32','DistanceBus': 'int32','request_time': 'int32','lat':'float','lon':'float'})
buses_data['datetime'] = pd.to_datetime(buses_data['datetime'], format='%Y-%m-%d %H:%M:%S.%f')

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
            value=[minutes_range-30, minutes_range]
        ),
        html.Div(className='columns',children = [
            html.Div(className='column',children = [
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
        html.Div(className='box',id='figs-selected-data'),
        html.Div(className='box', children = [
            html.Div(className='box',id='time-reduced-pos-control', children = [
                html.H2(
                    'Percentage of interval slider',
                    className = 'subtitle is-5'
                ),
                dcc.Slider(
                    id = 'time-reduced-slider',
                    min=0,
                    max=100,
                    marks={i*5: '{}%'.format(i*5) for i in range(21) },
                    step=1,
                    value=25
                )
            ]),
            html.Div(className='box',id='figs-buses-trayectory')
        ])
    ])
    
])

# FUNCTIONS 
def point_by_distance_on_line (line, destination, distance, stop_id) :
    """
    Returns the coordinates of the bus location

        Parameters
        ----------
        line : string
            The line that the bus belongs to
        destination : float
            The destination of the bus
        distance : float
            The distance of the bus to the stop in kilometers
        stop_id : string
            The id of the bus stop
    """
    
    #We get the selected lines from the dataframe
    lines_selected = route_lines.loc[route_lines['line_id'].isin(['1','82','91','92','99','132','502','506'])]
    
    #Get the line rows
    if line == '1' :
        line1 = lines_selected.loc[(route_lines['line_id']=='1')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='1')&(route_lines['direction']=='2')]
    elif line == '82' :
        line1 = lines_selected.loc[(route_lines['line_id']=='82')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='82')&(route_lines['direction']=='2')]
    elif line == 'F' :
        line1 = lines_selected.loc[(route_lines['line_id']=='91')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='91')&(route_lines['direction']=='2')]
    elif line == 'G' :     
        line1 = lines_selected.loc[(route_lines['line_id']=='92')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='92')&(route_lines['direction']=='2')]
    elif line == 'U' :
        line1 = lines_selected.loc[(route_lines['line_id']=='99')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='99')&(route_lines['direction']=='2')]
    elif line == '132' :    
        line1 = lines_selected.loc[(route_lines['line_id']=='132')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='132')&(route_lines['direction']=='2')]
    elif line == 'N2' :   
        line1 = lines_selected.loc[(route_lines['line_id']=='502')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='502')&(route_lines['direction']=='2')]
    elif line == 'N6' :    
        line1 = lines_selected.loc[(route_lines['line_id']=='506')&(route_lines['direction']=='1')]
        line2 = lines_selected.loc[(route_lines['line_id']=='506')&(route_lines['direction']=='2')]
    
    dests_1 = ['HOSPITAL LA PAZ','CIUDAD UNIVERSITARIA','PARANINFO','PITIS','PROSPERIDAD','VALDEBEBAS','LAS ROSAS']
    if destination in dests_1 :
        line = line1['geometry']
        line_length = line1['dist']
    else :
        line = line2['geometry']
        line_length = line2['dist']
    #We get the stop coords
    origin_point = stops.loc[stops['stop_code'] == stop_id].iloc[0]['geometry']
    
    #First we calculate the normalized distance of the bus from the start of the line
    #by substracting the distance of the bus to the stop to the distance of the stop to the start of the line
    #which is returned by the project method of the shapely module
    normalized_distance = line.project(origin_point,normalized=True) - distance/line_length
    
    #Then we get the the coordinates of the point that is at the normalized distance obtained 
    #before from the start of the line with the interpolate method
    interpolated_point = line.interpolate(normalized_distance,normalized=True)
    
    #And we return the coordinates of the point
    return interpolated_point.x,interpolated_point.y

# CALLBACKS

# CALLBACK 1 - Retrieved data
@app.callback([
        Output(component_id = 'selected-data',component_property = 'children')
    ],
    [
        Input(component_id = 'time-range-slider',component_property = 'value'),
        Input(component_id = 'lines-select',component_property = 'value'),
        Input(component_id = 'stops-select',component_property = 'value'),
        Input(component_id = 'buses-select',component_property = 'value'),
        Input(component_id = 'distance-range-radio',component_property = 'value'),
        Input(component_id = 'distance-range-slider',component_property = 'value'),
        Input(component_id = 'eta-range-radio',component_property = 'value'),
        Input(component_id = 'eta-range-slider',component_property = 'value')
    ])

def update_table(time_range,lines_selected,stops_selected,buses_selected,distance_radio,distance_range,eta_radio,eta_range):
        
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
                return ['No rows found for the selection']
            
            #And finally we return the graph element
            return [
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
            ]
        except :
            return ['No rows found for the selection']
        
# CALLBACK 2 - Representations of selected data
@app.callback(
        Output(component_id = 'figs-selected-data', component_property = 'children'),
    [
        Input(component_id = 'table', component_property = 'derived_virtual_data')
    ])
def update_graphs(rows):
    
    if rows == None :
        return 'Selected data is too large to represent. Select a smaller slice'
    
    if len(rows) < 500 :
        buses_data_reduced = pd.DataFrame(rows)
        buses_data_reduced['datetime'] = pd.to_datetime(buses_data_reduced['datetime'], format='%Y-%m-%dT%H:%M:%S.%f')
        buses_data_reduced['line'] = buses_data_reduced['line'].astype(str)
        
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

# CALLBACK 3 - BUSES TRAYECTORY OVER TIME
@app.callback(
        Output(component_id = 'figs-buses-trayectory', component_property = 'children'),
    [
        Input(component_id = 'table', component_property = 'derived_virtual_data'),
        Input(component_id = 'time-range-slider', component_property = 'value'),
        Input(component_id = 'time-reduced-slider', component_property = 'value')
    ])
def update_positions(rows,time_range,time_value):
    
    if rows == None :
        return 'Selected data is too large to represent. Select a smaller slice'
    
    if len(rows) < 200 :
        minutes = time_range[0] + time_value*(time_range[1]-time_range[0])/100
        time_threshold = start_date + timedelta(minutes=minutes)
        
        buses_data_reduced = pd.DataFrame(rows)
        buses_data_reduced['datetime'] = pd.to_datetime(buses_data_reduced['datetime'], format='%Y-%m-%dT%H:%M:%S.%f')
        #Get the rows that are inside the time interval
        mask = (buses_data_reduced['datetime'] < time_threshold)
        buses_data_reduced = buses_data_reduced.loc[mask]
        buses_in_df = buses_data_reduced['bus'].unique().tolist()
        center_x = buses_data_reduced['lon'].mean()
        center_y = buses_data_reduced['lat'].mean()
        
        #We create the figure object
        fig_buses_trayectory = go.Figure()
        
        for bus in buses_in_df :
            bus_df = buses_data_reduced.loc[buses_data_reduced['bus']==bus]
            bus_bools = bus_df['given_coords'].tolist()
            bus_lats = bus_df['lat'].tolist()
            bus_lons = bus_df['lon'].tolist()
            line = bus_df.iloc[0]['line']
            destination = bus_df.iloc[0]['destination']
            #We construct given and calculated coord lists
            lats_given = []
            lons_given = []
            lats_calc = [] 
            lons_calc = []
            
            last_given_lat,last_given_lon = None,None
            last_calc_lat,last_calc_lon = None,None
            for i in range(len(bus_bools)) :
                if bus_bools[i] == 1 :
                    lats_given.append(bus_lats[i])
                    lons_given.append(bus_lons[i])
                    last_given_lat = bus_lats[i]
                    last_given_lon = bus_lons[i]
            for index,row in bus_df.iterrows() :
                if row['estimateArrive'] < 10000 : 
                    calc_point = Point(point_by_distance_on_line(row['line'], row['destination'], row['DistanceBus']/1000, row['stop']))
                    lats_calc.append(calc_point.y)
                    lons_calc.append(calc_point.x)
                    last_calc_lat = calc_point.y
                    last_calc_lon = calc_point.x
            
            #Trace for given coords
            fig_buses_trayectory.add_trace(go.Scattermapbox(
                lat=lats_given,
                lon=lons_given,
                mode='lines',
                line=dict(width=2),
                name='{}-{}-{}'.format(line,bus,destination),
                text='{}-{}-{}'.format(line,bus,destination),
                hoverinfo='text'
            ))
            #Bus point for calc coords
            if not last_given_lat == None :
                fig_buses_trayectory.add_trace(go.Scattermapbox(
                    lat=[last_given_lat],
                    lon=[last_given_lon],
                    mode='markers',
                    marker=go.scattermapbox.Marker(
                        size=7,
                        color='#A9A9A9',
                        opacity=1
                    ),
                    name='{}-{}-{}'.format(line,bus,destination),
                    text='{}-{}-{}'.format(line,bus,destination),
                    hoverinfo='text'
                ))
            #Trace for calc coords
            fig_buses_trayectory.add_trace(go.Scattermapbox(
                lat=lats_calc,
                lon=lons_calc,
                mode='lines',
                line=dict(width=2),
                name='{}-{}-{}'.format(line,bus,destination),
                text='{}-{}-{}'.format(line,bus,destination),
                hoverinfo='text'
            ))
            #Bus point for calc coords
            if not last_calc_lat == None :
                fig_buses_trayectory.add_trace(go.Scattermapbox(
                    lat=[last_calc_lat],
                    lon=[last_calc_lon],
                    mode='markers',
                    marker=go.scattermapbox.Marker(
                        size=7,
                        color='black',
                        opacity=1
                    ),
                    name='{}-{}-{}'.format(line,bus,destination),
                    text='{}-{}-{}'.format(line,bus,destination),
                    hoverinfo='text'
                ))
        #Set figure layout
        fig_buses_trayectory.update_layout(
            title='Trayectory of buses until selected time',
            height=500,
            margin=dict(r=0, l=0, t=0, b=0),
            hovermode='closest',
            showlegend=True,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=dict(
                    lat=center_y,
                    lon=center_x
                ),
                pitch=0,
                zoom=12,
                style=style_day
            )
        )
            
        return [
            html.H2(
                'Trayectory of buses until selected time',
                className = 'subtitle is-4'
            ),
            dcc.Graph(
                id='fig-buses-trayectory',
                figure=fig_buses_trayectory
            )
        ]
    else :
        return 'Selected data is too large to represent. Select a smaller slice'