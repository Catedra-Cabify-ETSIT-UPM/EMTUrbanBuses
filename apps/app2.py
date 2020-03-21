import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
import json

import plotly.graph_objects as go
from shapely import wkt
import math

from datetime import datetime as dt
from datetime import timedelta

from app import app

# WE LOAD THE DATA
stops = pd.read_json('M6Data/stops.json')
lines_shapes = pd.read_json('M6Data/lines_shapes.json')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)

#Token for the mapbox api
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style_day = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
style_night = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'

#Load the buses dataframe and parse the dates ../../flash/EMTBuses/buses_data.csv
buses_data = pd.read_csv(
    '../buses_data.csv',
    dtype={
        'line': 'str',
        'destination': 'str',
        'stop': 'int32',
        'bus': 'int32',
        'given_coords': 'int32',
        'pos_in_burst':'int32',
        'deviation': 'int32',
        'estimateArrive': 'int32',
        'DistanceBus': 'int32',
        'request_time': 'int32',
        'lat':'float',
        'lon':'float'
    }
)
buses_data['datetime'] = pd.to_datetime(buses_data['datetime'], format='%Y-%m-%d %H:%M:%S.%f')

#Values for components
lines_retrieved = ['1','82','F','G','U','132','N2','N6']
buses_retrieved = buses_data.bus.unique().tolist()
stops_retrieved = buses_data.stop.unique().tolist()

layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('DATA RETRIEVED MONITORING',className = 'title is-3'),
        html.Div(className='columns', children = [
            html.Div(className='column is-narrow', children=[
                dcc.DatePickerRange(
                    id='date-picker-range',
                    display_format='MMM Do, YY',
                    min_date_allowed=buses_data.datetime.min(),
                    max_date_allowed=buses_data.datetime.max(),
                    start_date=buses_data.datetime.max().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days = 1),
                    end_date=buses_data.datetime.max().replace(hour=0, minute=0, second=0, microsecond=0)
                )
            ]),
            html.Div(className='column', children=[
                html.Span('Date and hours interval: ', className = 'tag is-light is-medium'),
                dcc.RangeSlider(
                    id='time-range-slider',
                    marks={i: '{}h'.format(i) for i in range(25)},
                    min=0,
                    max=24,
                    step=0.1,
                    value=[19,20]
                )
            ])
        ]),
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

# CALLBACKS

# CALLBACK 1 - Retrieved data
@app.callback([
        Output(component_id = 'selected-data',component_property = 'children')
    ],
    [
        Input(component_id = 'date-picker-range',component_property = 'start_date'),
        Input(component_id = 'date-picker-range',component_property = 'end_date'),
        Input(component_id = 'time-range-slider',component_property = 'value'),
        Input(component_id = 'lines-select',component_property = 'value'),
        Input(component_id = 'stops-select',component_property = 'value'),
        Input(component_id = 'buses-select',component_property = 'value'),
        Input(component_id = 'distance-range-radio',component_property = 'value'),
        Input(component_id = 'distance-range-slider',component_property = 'value'),
        Input(component_id = 'eta-range-radio',component_property = 'value'),
        Input(component_id = 'eta-range-slider',component_property = 'value')
    ])

def update_table(start_date,end_date,time_range,lines_selected,stops_selected,buses_selected,distance_radio,distance_range,eta_radio,eta_range):

    try :
        showAllLines = False
        showAllStops = False
        showAllBuses = False
        showAllDistances = False
        showAllEtas = False

        if len(start_date) < 12 :
            start_date = dt.strptime(start_date, '%Y-%m-%d')
        else :
            start_date = dt.strptime(start_date, '%Y-%m-%dT%H:%M:%S')

        if len(end_date) < 12 :
            end_date = dt.strptime(end_date, '%Y-%m-%d')
        else :
            end_date = dt.strptime(end_date, '%Y-%m-%dT%H:%M:%S')

        mins = int((end_date-start_date).total_seconds()/60)
        #We add the minutes and get the start and end of the time interval
        start_interval = start_date + timedelta(minutes=mins*(time_range[0]/24))
        end_interval = start_date + timedelta(minutes=mins*(time_range[1]/24))

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
            fs_bus_df = fs_df.loc[fs_df.bus==bus]
            line = fs_bus_df.iloc[0].line
            bus_times = fs_bus_df.datetime.tolist()
            bus_dists = [dist/1000 for dist in fs_bus_df.DistanceBus.tolist()]
            bus_etas = [eta/60 for eta in fs_bus_df.DistanceBus.tolist()]

            bus_times_splitted,bus_dists_splitted,bus_etas_splitted,bus_etas_error_splitted = [],[],[],[]
            last_distance = bus_dists[0]
            last_index = 0
            for i in range(len(bus_dists)) :
                if ((bus_dists[i] - last_distance)>1) | (i==len(bus_dists)-1) :
                    bus_times_splitted.append(bus_times[last_index:i])
                    bus_dists_splitted.append(bus_dists[last_index:i])
                    bus_etas_splitted.append(bus_etas[last_index:i])
                    #Bus etas error
                    bus_etas_error = []
                    last_time = bus_times[i-1]
                    if bus_etas[i-1] <= 1 :
                        for k in range(last_index,i) :
                            error = (bus_times[k] + timedelta(minutes=bus_etas[k])) - last_time
                            error_mins = error.total_seconds()/60
                            bus_etas_error.append(error_mins)
                        bus_etas_error_splitted.append(bus_etas_error)

                    last_index = i
                last_distance = bus_dists[i]

            # Create and style traces
            for i in range(len(bus_times_splitted)) :
                fig1.add_trace(go.Scatter(x=bus_times_splitted[i], y=bus_dists_splitted[i], name='{}-{}'.format(line,bus),
                                         line=dict(width=1)))
                fig2.add_trace(go.Scatter(x=bus_times_splitted[i], y=bus_etas_splitted[i], name='{}-{}'.format(line,bus),
                                         line=dict(width=1)))
            for i in range(len(bus_etas_error_splitted)) :
                fig3.add_trace(go.Scatter(x=bus_times_splitted[i], y=bus_etas_error_splitted[i], name='{}-{}'.format(line,bus),
                                         line=dict(width=1)))
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
        Input(component_id = 'time-reduced-slider', component_property = 'value')
    ])
def update_positions(rows,time_value):

    if rows == None :
        return 'Selected data is too large to represent. Select a smaller slice'

    if len(rows) < 200 :

        buses_data_reduced = pd.DataFrame(rows)
        buses_data_reduced['datetime'] = pd.to_datetime(buses_data_reduced['datetime'], format='%Y-%m-%dT%H:%M:%S.%f')

        #Get the rows that are inside the time interval
        minutes = (buses_data_reduced.datetime.max()-buses_data_reduced.datetime.min()).total_seconds()/60
        time_threshold = buses_data_reduced.datetime.min() + timedelta(minutes=minutes*(time_value/100))
        mask = (buses_data_reduced['datetime'] < time_threshold)
        buses_data_reduced = buses_data_reduced.loc[mask]
        buses_in_df = buses_data_reduced.bus.unique().tolist()
        center_x = buses_data_reduced.lon.mean()
        center_y = buses_data_reduced.lat.mean()

        #We create the figure object
        fig_buses_trayectory = go.Figure()

        for bus in buses_in_df :
            bus_df = buses_data_reduced.loc[buses_data_reduced.bus==bus]
            max_time = bus_df.datetime.max()
            min_time = max_time - timedelta(minutes=15)
            mask = (bus_df.datetime>min_time)&(bus_df.datetime<max_time)
            bus_df = bus_df.loc[mask]
            if bus_df.shape[0] != 0 :
                bus_bools = bus_df.given_coords.tolist()
                bus_lats = bus_df.lat.tolist()
                bus_lons = bus_df.lon.tolist()
                line = bus_df.iloc[0].line
                destination = bus_df.iloc[0].destination
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

                dests_1 = ['HOSPITAL LA PAZ','CIUDAD UNIVERSITARIA','PARANINFO','PITIS','PROSPERIDAD','VALDEBEBAS','LAS ROSAS','1']
                for row in bus_df.itertuples() :
                    if row.estimateArrive < 10000 :
                        if row.destination in dests_1 :
                            line_rows = lines_shapes.loc[(lines_shapes.line_sn == row.line)&(lines_shapes.direction == 1)]
                        else :
                            line_rows = lines_shapes.loc[(lines_shapes.line_sn == row.line)&(lines_shapes.direction == 2)]
                        stop  = stops.loc[stops.id == row.stop].iloc[0]
                        last_calc_lon,last_calc_lat = point_by_distance_on_line(line_rows, row.DistanceBus, stop.lat, stop.lon)
                        lats_calc.append(last_calc_lat)
                        lons_calc.append(last_calc_lon)

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
                #Bus point for given coords
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
                zoom=14,
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
