import dash_core_components as dcc
import dash_html_components as html
import dash_table

from dash.dependencies import Input, Output

import pandas as pd
import json

import plotly.graph_objects as go
import plotly.io as pio

import math

from scipy import stats
from scipy.spatial.distance import mahalanobis
from scipy.stats.distributions import chi2

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
    '1': 12.3,
    '44': 11.8,
    '82': 11.8,
    'F': 13,
    'G': 13,
    'U': 13,
    '132': 11.5,
    '133': 11.5,
    'N2': 11.5,
    'N6': 11.5,
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

    html.Div(className = '', children = [
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
    df_reduced = df.loc[(df.dist_traveled>dist_traveled-300)&(df.dist_traveled<dist_traveled+300)]
    if df_reduced.shape[0]!=0:
        for row in df_reduced.itertuples() :
            error = abs(row.dist_traveled-dist_traveled)
            if  error < min_dist_error :
                min_dist_error = error
                nearest_row = row
    else :
        nearest_row = df.iloc[0]
    return nearest_row



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

    #And set the figure layout
    new_map.update_layout(
        title='<b>BUSES POSITION</b>',
        height=350,
        margin=dict(r=0, l=0, t=40, b=0),
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

    #And finally we return the map
    return new_map


def build_graph(line_hws) :
    '''
    Returns a figure with the graph of headways between buses
    '''

    #Process headways
    headways = line_hws

    #Create figure object
    graph = go.Figure()

    #Set title and layout
    graph.update_layout(
        title='<b>HEADWAYS</b>',
        legend_title='<b>Bus ids</b>',
        xaxis = dict(
            title_text = 'Seconds remaining to last stop of the line',
            nticks=20
        ),
        height=350,
        margin=dict(r=0, l=0, t=40, b=0),
        hovermode='closest'
    )

    if headways.shape[0] < 1 :
        return graph

    #Destinations
    line = headways.line.iloc[0]
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

    #Finally we return the graph
    return graph


def build_m_dist_graph(series_df) :

    graph = go.Figure()

    #Set title and layout
    graph.update_layout(
        title='<b>MAHALANOBIS DISTANCE</b>',
        legend_title='<b>Group ids</b>',
        xaxis = dict(
            nticks=20
        ),
        yaxis = dict(
            title_text = 'Mahalanobis Distance',
            nticks=20
        ),
        height=350,
        margin=dict(r=0, l=0, t=40, b=0),
        hovermode='closest'
    )

    if series_df.shape[0] < 1 :
        return graph

    #All bus names
    bus_names_all = ['bus' + str(i) for i in range(1,8+2)]
    hw_names_all = ['hw' + str(i) + str(i+1) for i in range(1,8+1)]

    #Min and max datetimes
    min_time = series_df.datetime.min()
    max_time = series_df.datetime.max()

    #Locate unique groups
    unique_groups = []
    unique_groups_df = series_df.drop_duplicates(bus_names_all)
    for i in range(unique_groups_df.shape[0]):
        group = [unique_groups_df.iloc[i][bus_names_all[k]] for k in range(8+1)]
        unique_groups.append(group)

    last_dim = 0
    for group in unique_groups :
        #Build indexing conditions
        conds = [series_df[bus_names_all[k]] == group[k] for k in range(8+1)]
        final_cond = True
        for cond in conds :
            final_cond &= cond
        group_df = series_df.loc[final_cond]
        group_df = group_df.sort_values('datetime')

        #Dimension
        dim = group_df.iloc[0].dim
        color = colors[dim]

        #Dim threshold
        conf = 0.95
        m_th = math.sqrt(chi2.ppf(conf, df=dim))

        if dim != last_dim :
            graph.add_shape(
                name='{}Dim MD Threshold'.format(dim),
                type='line',
                x0=min_time,
                y0=m_th,
                x1=max_time,
                y1=m_th,
                line=dict(
                    color=color,
                    width=2,
                    dash='dashdot',
                ),
            )

        last_dim = dim

        name = str(group[0])
        for bus in group[1:] :
            if bus != 0 :
                name+='-'+str(bus)
            else :
                break

        #Build group trace
        graph.add_trace(go.Scatter(
            name=name,
            x=group_df.datetime,
            y=group_df.m_dist,
            mode='lines+markers',
            line=dict(width=1.5, color=color)
        ))

    return graph



def build_anoms_table(anomalies_df) :

    #All bus names
    bus_names_all = ['bus' + str(i) for i in range(1,8+2)]
    hw_names_all = ['hw' + str(i) + str(i+1) for i in range(1,8+1)]

    if anomalies_df.shape[0] < 1 :
        return 'No anomalies were detected yet.'

    #Drop group duplicates
    anomalies_df = anomalies_df.sort_values('datetime',ascending=False).drop_duplicates(bus_names_all,keep='first')

    #Build group names
    names = []
    for i in range(anomalies_df.shape[0]):
        group = [anomalies_df.iloc[i][bus_names_all[k]] for k in range(8+1)]
        name = str(group[0])
        for bus in group[1:] :
            if bus != 0 :
                name+='-'+str(bus)
            else :
                break

        names.append(name)

    anomalies_df['group'] = names
    anomalies_df = anomalies_df[['dim','group','anom_size','datetime']]

    table = dash_table.DataTable(
        id='table',
        filter_action='native',
        sort_action='native',
        page_size= 5,
        style_header={
            'color':'white',
            'backgroundColor': 'lightseagreen'
        },
        style_cell={
            'padding': '3px',
            'width': 'auto',
            'textAlign': 'center',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
        },
        columns=[{"name": i, "id": i} for i in anomalies_df.columns],
        data=anomalies_df.to_dict('records')
    )

    return table



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


    #Read last processed headways
    hws_burst = pd.read_csv('ProcessedData/headways_burst.csv',
        dtype={
            'line': 'str',
            'direction': 'uint16',
            'busA': 'uint16',
            'busB': 'uint16',
            'headway':'uint16',
            'busB_ttls':'uint16'
        }
    )[['line','direction','datetime','hw_pos','busA','busB','headway','busB_ttls']]

    #Read last series data
    series_df = pd.read_csv('ProcessedData/series.csv',
        dtype={
            'line': 'str'
        }
    )

    #Read last anomalies data
    anomalies_df = pd.read_csv('ProcessedData/anomalies.csv',
        dtype={
            'line': 'str'
        }
    )


    #Lines to iterate over
    lines = ['1','44','82','F','G','U','132','133','N2','N6']
    maps,graphs,m_dist_graphs,anoms_tables = [],[],[],[]
    for line in lines :
        #Line dataframe
        line_df = burst.loc[burst.line == line]
        line_hws = hws_burst.loc[hws_burst.line == line]
        line_series = series_df.loc[series_df.line == line]
        line_anoms = anomalies_df.loc[anomalies_df.line == line]

        #Create map
        new_map = build_map(line_df)
        maps.append(new_map)

        #Create graph
        graph = build_graph(line_hws)
        graphs.append(graph)

        #Create mh dist graph
        m_dist_graph = build_m_dist_graph(line_series)
        m_dist_graphs.append(m_dist_graph)

        #Create anomalies table
        anoms_table = build_anoms_table(line_anoms)
        anoms_tables.append(anoms_table)

    #Build tag objects
    tabs = []
    for i in range(len(lines)) :
        if maps[i] == 'EMPTY' :
            tab = dcc.Tab(label=lines[i],children = [
                html.H2('NO BUSES WERE FOUND IN THE LINE',className = 'subtitle is-3')
            ],style={'padding': '0','line-height': '5vh'},selected_style={'padding': '0','line-height': '5vh'})
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
                ]),
                html.Div(className='columns',children=[
                    html.Div(className='column is-half',children=[
                        dcc.Graph(
                            id = 'graph-mdist-{}'.format(lines[i]),
                            figure = m_dist_graphs[i]
                        )
                    ]),
                    html.Div(className='column is-half',children=[
                        html.Div([
                            html.H2('Detected anomalies',className = 'subtitle is-5'),
                            anoms_tables[i]
                        ])
                    ])
                ])
            ],style={'padding': '0','line-height': '5vh'},selected_style={'padding': '0','line-height': '5vh'})
        tabs.append(tab)

    #And return all of them
    return [
        dcc.Tabs(tabs,style={
            'font-size': '150%',
            'height':'5vh'
        })
    ]
