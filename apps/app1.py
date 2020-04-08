import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import pandas as pd
import json

import plotly.graph_objects as go
import plotly.io as pio

import math

from app import app

#Available colors
colors = [
    '#1f77b4',  # muted blue
    '#ff7f0e',  # safety orange
    '#2ca02c',  # cooked asparagus green
    '#d62728',  # brick red
    '#9467bd',  # muted purple
    '#8c564b',  # chestnut brown
    '#e377c2',  # raspberry yogurt pink
    '#7f7f7f',  # middle gray
    '#bcbd22',  # curry yellow-green
    '#17becf'   # blue-teal
]

zooms = {
    '1': 12.8,
    '44': 13,
    '82': 13,
    'F': 14,
    'G': 14,
    'U': 14,
    '132': 13,
    '133': 13,
    'N2': 12.5,
    'N6': 12.5,
}

# WE LOAD THE DATA
stops = pd.read_csv('M6Data/stops.csv')
lines_shapes = pd.read_csv('M6Data/lines_shapes.csv')
with open('M6Data/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)
#Load times between stops data
times_bt_stops = pd.read_csv('../../flash/EMTBuses/ProcessedData/times_bt_stops.csv',
    dtype={
        'line': 'str',
        'direction': 'uint16',
        'st_hour': 'uint16',
        'end_hour': 'uint16',
        'stopA': 'uint16',
        'stopB': 'uint16',
        'bus': 'uint16',
        'trip_time':'float32',
        'api_trip_time':'float32'
    }
)
#Parse the dates
times_bt_stops['date'] = pd.to_datetime(times_bt_stops['date'], format='%Y-%m-%d')


layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('LIVE DATA MONITORING',className = 'title is-3'),
        html.H2('Direction 1 (from A to B) of the lines is represented in blue, and direction 2 in red. The stops are represented in green and \
        each bus has an unique color asociated.',className = 'subtitle is-4'),
        html.Div(className='box',id='live-update-data'),
        dcc.Interval(
            id='interval-component',
            interval=50*1000, # in milliseconds
            n_intervals=0
        )
    ])

])

#MAPBOX API TOKEN AND STYLES
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
pio.templates.default = 'plotly_white'


# FUNCTIONS
def calculate_coords(df,stop_id,dist_to_stop) :
    '''
    Returns the calculated coordinates of the bus
    Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the calculated coords
        stop : str
        dist_traveled : float
    '''
    line_sn = df.iloc[0].line_sn
    direction = str(df.iloc[0].direction)
    bus_distance = int(lines_collected_dict[line_sn][direction]['distances'][str(stop_id)]) - dist_to_stop
    nearest_row = find_nearest_row_by_dist(df,bus_distance)
    return nearest_row.lon, nearest_row.lat

def find_nearest_row_by_dist(df,dist_traveled) :
    '''
    Returns the row nearest to the distance traveled passed in the dataframe
        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        dist_traveled : float
    '''
    min_dist_error = 1000000.0
    df_reduced = df.loc[(df.dist_traveled>dist_traveled-100)&(df.dist_traveled<dist_traveled+100)]
    if df_reduced.shape[0]!=0:
        for row in df_reduced.itertuples() :
            error = abs(row.dist_traveled-dist_traveled)
            if  error < min_dist_error :
                min_dist_error = error
                nearest_row = row
    else :
        nearest_row = df.iloc[0]
    return nearest_row

def process_headways(int_df) :
    rows_list = []
    #Burst time
    actual_time = int_df.iloc[0].datetime
    
    #Line
    line = int_df.iloc[0].line
    
    #Stops of each line reversed
    stops1 = lines_collected_dict[line]['1']['stops'][-2::-1]
    stops2 = lines_collected_dict[line]['2']['stops'][-2::-1]
    
    #Assign destination values
    dest2,dest1 = lines_collected_dict[line]['destinations']
    
    #Process mean times between stops
    tims_bt_stops = times_bt_stops.loc[(times_bt_stops.line == line) & \
                                        (times_bt_stops.date.dt.weekday == actual_time.weekday()) & \
                                        (times_bt_stops.st_hour >= actual_time.hour) & \
                                        (times_bt_stops.st_hour <= actual_time.hour + 3)]
    #Group and get the mean values
    tims_bt_stops = tims_bt_stops.groupby(['line','direction','stopA','stopB']).mean()
    tims_bt_stops = tims_bt_stops.reset_index()[['line','direction','stopA','stopB','trip_time','api_trip_time']]

    #All stops of the line
    stops = stops1 + stops2
    stop_df_list = []
    buses_out1,buses_out2 = [],[]
    dest,direction = dest1,1
    for i in range(len(stops)) :
        stop = stops[i]
        if i == 0 :
            mean_time_to_stop = 0
        elif i == len(stops1) :
            mean_time_to_stop = 0
            dest,direction = dest2,2
        else :
            mean_df = tims_bt_stops.loc[(tims_bt_stops.stopA == int(stop)) & \
                            (tims_bt_stops.direction == direction)]
            if mean_df.shape[0] > 0 :
                mean_time_to_stop += mean_df.iloc[0].trip_time
            else :
                break

        stop_df = int_df.loc[(int_df.stop == int(stop)) & \
                            (int_df.destination == dest)]

        #Drop duplicates, recalculate estimateArrive and append to list
        stop_df = stop_df.drop_duplicates('bus',keep='first')
        
        if (stop == stops1[-1]) or (stop == stops2[-1]) :
            if direction == 1 :
                buses_out1 += stop_df.bus.unique().tolist()
            else :
                buses_out2 += stop_df.bus.unique().tolist()
        if (stop == stops1[0]) or (stop == stops2[0]) :
            buses_near = stop_df.loc[stop_df.estimateArrive < 5]
            if buses_near.shape[0] > 0 :
                if direction == 1 :
                    buses_out1 += buses_near.bus.unique().tolist()
                else :
                    buses_out2 += buses_near.bus.unique().tolist()
        else :
            stop_df.estimateArrive = stop_df.estimateArrive + mean_time_to_stop
            stop_df_list.append(stop_df)
            
    #Concatenate and group them
    stops_df = pd.concat(stop_df_list)

    #Group by bus and destination
    stops_df = stops_df.groupby(['bus','destination']).mean().sort_values(by=['estimateArrive'])
    stops_df = stops_df.reset_index().drop_duplicates('bus',keep='first').sort_values(by=['destination'])
    #Loc buses not given by first stop
    stops_df = stops_df.loc[((stops_df.destination == dest1) & (~stops_df.bus.isin(buses_out1))) | \
                            ((stops_df.destination == dest2) & (~stops_df.bus.isin(buses_out2))) ]
    
    #Calculate time intervals
    if stops_df.shape[0] > 0 :
        hw_pos1 = 0
        hw_pos2 = 0
        for i in range(stops_df.shape[0]) :
            est1 = stops_df.iloc[i]

            direction = 1 if est1.destination == dest1 else 2
            if ((direction == 1) & (hw_pos1 == 0)) or ((direction == 2) & (hw_pos2 == 0))  :
                #Create dataframe row
                row = {}
                row['datetime'] = actual_time
                row['line'] = line
                row['direction'] = direction
                row['busA'] = 0
                row['busB'] = est1.bus
                row['hw_pos'] = 0
                row['headway'] = 0
                row['busB_ttls'] = int(est1.estimateArrive)

                #Append row to the list of rows
                rows_list.append(row)

                #Increment hw pos
                if direction == 1 :
                    hw_pos1 += 1
                else :
                    hw_pos2 += 1

            if i < (stops_df.shape[0] - 1) :
                est2 = stops_df.iloc[i+1]
            else :
                break

            if est1.destination == est2.destination :
                headway = int(est2.estimateArrive-est1.estimateArrive)

                #Create dataframe row
                row = {}
                row['datetime'] = actual_time
                row['line'] = line
                row['direction'] = direction
                row['busA'] = est1.bus
                row['busB'] = est2.bus
                row['hw_pos'] = hw_pos1 if direction == '1' else hw_pos2
                row['headway'] = headway
                row['busB_ttls'] = int(est2.estimateArrive)

                #Append row to the list of rows
                rows_list.append(row)

                #Increment hw pos
                if direction == 1 :
                    hw_pos1 += 1
                else :
                    hw_pos2 += 1
                    
    return pd.DataFrame(rows_list)
                    
def build_map(line_df) :
    '''
    Returns a figure with the map of live location of buses
    '''
    if line_df.shape[0] < 1 :
        return 'EMPTY'
    
    #Line and destinations
    line = line_df.iloc[0].line
    dest2,dest1 = lines_collected_dict[line]['destinations']
    
    #Select line line shapes
    line1 = lines_shapes.loc[(lines_shapes.line_sn == line) & (lines_shapes.direction == 1)]
    line2 = lines_shapes.loc[(lines_shapes.line_sn == line) & (lines_shapes.direction == 2)]
    center_x = line1.lon.mean()
    center_y = line1.lat.mean()

    #We drop the duplicated buses keeping the instance that is closer to a stop
    line_df = line_df.sort_values(by='DistanceBus').drop_duplicates('bus',keep='first')

    #We create the figure object
    new_map = go.Figure()
    
    #Add the bus points to the figure
    for bus in line_df.itertuples() :
        if bus.destination == dest1 :
            lon,lat = calculate_coords(line1,bus.stop,bus.DistanceBus)
        else :
            lon,lat = calculate_coords(line2,bus.stop,bus.DistanceBus)
            
        #Assign color based on bus id
        color = colors[bus.bus%len(colors)]
        #Bus marker
        new_map.add_trace(go.Scattermapbox(
            lat=[lat],
            lon=[lon],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=20,
                color=color,
                opacity=1
            ),
            text=[bus.bus],
            hoverinfo='text'
        ))

    #Select line stops
    stop_names = lines_collected_dict[line]['1']['stops'][1:] + lines_collected_dict[line]['2']['stops'][1:]
    line_stops = stops.loc[stops.id.isin(stop_names)]

    #Add the stops to the figure
    new_map.add_trace(go.Scattermapbox(
        lat=line_stops.lat,
        lon=line_stops.lon,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=8,
            color='green',
            opacity=0.7
        ),
        text=line_stops.id,
        hoverinfo='text'
    ))
    
    #Add lines to the figure
    for line_shape in [line1,line2] :
        color = 'rgb(108, 173, 245)' if line_shape.iloc[0].direction == 1 else 'rgb(243, 109, 90)'
        new_map.add_trace(go.Scattermapbox(
            lat=line_shape.lat,
            lon=line_shape.lon,
            mode='lines',
            line=dict(width=1.5, color=color),
            text='Línea : {}-{}'.format(line,line_shape.iloc[0].direction),
            hoverinfo='text'
        ))

    #And set the figure layout
    new_map.update_layout(
        title='<b>BUSES POSITION</b>',
        height=500,
        margin=dict(r=0, l=0, t=50, b=0),
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
            zoom=zooms[line],
            style=style
        )
    )
    #And finally we return the map
    return new_map

def build_graph(line_df) :
    '''
    Returns a figure with the graph of headways between buses
    '''
    if line_df.shape[0] < 1 :
        return 'EMPTY'
    
    #Process headways
    headways = process_headways(line_df)
    
    #Create figure object
    graph = go.Figure()
    
    #Destinations
    line = line_df.line.iloc[0]
    dest2,dest1 = lines_collected_dict[line]['destinations']
    
    #Max dists
    hw1 = headways.loc[headways.direction == 1]
    hw2 = headways.loc[headways.direction == 2]
    if hw1.shape[0] == 0 :
        max_dist1 = 0
    else :
        max_dist1 = hw1.busB_ttls.max()
        #Add trace
        graph.add_trace(go.Scatter(
            x=hw1.busB_ttls,
            y=[('To: ' + dest1) for direction in hw1.direction.tolist()],
            mode='lines+markers',
            line=dict(width=1.5, color='rgb(108, 173, 245)'),
            showlegend=False
        ))
        
    if hw2.shape[0] == 0 :
        max_dist2 = 0
    else :
        max_dist2 = hw2.busB_ttls.max()
        #Add trace
        graph.add_trace(go.Scatter(
            x=hw2.busB_ttls,
            y=[('To: ' + dest2) for direction in hw2.direction.tolist()],
            mode='lines+markers',
            line=dict(width=1.5, color='rgb(243, 109, 90)'),
            showlegend=False
        ))

    #Add buses to graph
    for bus in headways.itertuples() :
        #Assign color based on bus id
        color = colors[bus.busB%len(colors)]
        
        if bus.direction == 1 :
            dest = dest1 
        else :
            dest = dest2
            
        #Add marker
        graph.add_trace(go.Scatter(
            mode='markers',
            name=bus.busB,
            x=[bus.busB_ttls],
            y=['To: ' + dest],
            marker=dict(
                size=20,
                color=color
            ),
            text=[str(bus.headway)+' seconds to next bus'],
            hoverinfo='text'
        ))
        
    #Set title and layout
    graph.update_layout(
        title='<b>HEADWAYS</b>',
        legend_title='<b>Bus ids</b>',
        xaxis = dict(
            title_text = 'Seconds remaining to last stop of the line',
            nticks=20
        ),
        height=500,
        margin=dict(r=0, l=0, t=50, b=0),
        hovermode='closest'
    )
        
    #Finally we return the graph
    return graph


# CALLBACKS

# CALLBACK 1 - Live Graph of Line Buses
@app.callback(Output(component_id = 'live-update-data',component_property = 'children'),
              [Input(component_id = 'interval-component',component_property = 'n_intervals')])
def update_graph_live(n_intervals) :
    '''
    Function that reads buses_data_burst every x seconds and updates the graphs

        Parameters
        ---
        input_lineId_value: string
            The line whose buses are going to be ploted
    '''
    #Read last burst of data
    burst = pd.read_csv('../../flash/EMTBuses/buses_data_burst.csv',
        dtype={
            'line': 'str',
            'destination': 'str',
            'stop': 'uint16',
            'bus': 'uint16',
            'given_coords': 'bool',
            'pos_in_burst':'uint16',
            'estimateArrive': 'int32',
            'DistanceBus': 'int32',
            'request_time': 'int32',
            'lat':'float32',
            'lon':'float32'
        }
    )[['line','destination','stop','bus','datetime','estimateArrive','DistanceBus']]
    #Parse the dates
    burst['datetime'] = pd.to_datetime(burst['datetime'], format='%Y-%m-%d %H:%M:%S.%f')

    #Lines to iterate over
    lines = ['1','44','82','F','G','U','132','133','N2','N6']
    maps,graphs = [],[]
    for line in lines :
        #Line dataframe
        line_df = burst.loc[burst.line == line]

        #Create map
        new_map = build_map(line_df)
        maps.append(new_map)

        #Create graph
        graph = build_graph(line_df)
        graphs.append(graph)

        #FUTURE : SHOULD CHECK DATA FOR ANOMALIES DETECTION

    #Build tag objects
    tabs = []
    for i in range(len(lines)) :
        if maps[i] == 'EMPTY' :
            tab = dcc.Tab(label=lines[i],children = [
                html.H2('NO BUSES WERE FOUND IN THE LINE',className = 'subtitle is-3')
            ])
        else :
            tab = dcc.Tab(label=lines[i],children = [
                html.Div(className='columns',children=[
                    html.Div(className='column is-half',children=[
                        dcc.Graph(
                            id = 'map-{}'.format(lines[i]),
                            figure = maps[i]
                        )
                    ]),
                    html.Div(className='column is-half',children=[
                        dcc.Graph(
                            id = 'graph-{}'.format(lines[i]),
                            figure = graphs[i]
                        )
                    ])
                ])
            ])
        tabs.append(tab)

    #And return all of them
    return [
        dcc.Tabs(tabs)
    ]

