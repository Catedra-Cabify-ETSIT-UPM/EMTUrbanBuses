import dash_core_components as dcc
import dash_html_components as html
import dash_table

from dash.dependencies import Input, Output

import pandas as pd
pd.options.mode.chained_assignment = None

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

colors2 = [
    "#023fa5", "#7d87b9", "#bec1d4", "#d6bcc0", "#bb7784", 
    "#8e063b", "#4a6fe3", "#8595e1", "#b5bbe3", "#e6afb9", 
    "#e07b91", "#d33f6a", "#11c638", "#8dd593", "#c6dec7", 
    "#ead3c6", "#f0b98d", "#ef9708", "#0fcfc0", "#9cded6", 
    "#d5eae7", "#f3e1eb", "#f6c4e1", "#f79cd4"
]

# WE LOAD THE DATA
stops = pd.read_csv('../Data/Static/stops.csv')
lines_shapes = pd.read_csv('../Data/Static/lines_shapes.csv')
with open('../Data/Static/lines_collected_dict.json', 'r') as f:
    lines_collected_dict = json.load(f)


layout = html.Div(className = '', children = [
    html.Div(className='box', children = [
        html.Div(className='columns', children=[
            html.Div(id='tab-title', className='column'),
            html.Div(id='conf',className='column', style=dict(height='7vh'), children=[
                dcc.Input(id="conf-slider", type="text", value=0, style={'display':'none'})
            ]),
            html.Div(id='size-th',className='column', style=dict(height='7vh'), children=[
                dcc.Input(id="size-th-slider", type="text", value=0, style={'display':'none'})
            ]),
            html.Div(className='column is-narrow', style=dict(height='7vh',width='7vh'),children=[
                dcc.Loading(id='new-interval-loading', type='dot', style=dict(height='7vh',width='7vh')),
            ]),
            html.Div(className='column is-narrow', style=dict(height='0.5vh'), children=[
                html.Button('Force Update',className='button', id='update-button')
            ])
        ]),
        html.Div(className='columns',children=[
            html.Div(id='buses-pos-div', className='column is-3', children=[
                dcc.Graph(
                    id = 'map',
                    className = 'box',
                    style=dict(height='39vh'),
                    figure = go.Figure()
                )
            ]),
            html.Div(id='flat-hws-div', className='column is-5', children=[
                dcc.Graph(
                    id = 'flat-hws',
                    className = 'box',
                    style=dict(height='39vh'),
                    figure = go.Figure(),
                    clear_on_unhover=True
                )
            ]),
            html.Div(id='time-series-hws-div', className='column is-4', children=[
                dcc.Graph(
                    id = 'time-series-hws',
                    className = 'box',
                    style=dict(height='39vh'),
                    figure = go.Figure()
                )
            ]),
        ]),
        html.Div(className='columns',children=[
            html.Div(id='mdist-hws-div', className='column is-half'),
            html.Div(id='anom-hws-div',className='column is-half')
        ])
    ]),
    html.Div(id='hidden-div', style={'display':'none'}),
    dcc.Interval(
        id='interval-component',
        interval=30*1000, # in milliseconds
        n_intervals=0
    )
])

#MAPBOX API TOKEN AND STYLES
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style = 'mapbox://styles/alejp1998/ck9voa0bb002y1ipcx8j00oeu'
pio.templates.default = 'plotly_white'


# FUNCTIONS
def read_df(name) :
    if name == 'burst' :
        #Read last burst of data
        df = pd.read_csv('../Data/RealTime/buses_data_burst_c.csv',
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
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
    elif name == 'hws_burst' :
        #Read last processed headways
        df = pd.read_csv('../Data/RealTime/headways_burst.csv',
            dtype={
                'line': 'str',
                'direction': 'uint16',
                'busA': 'uint16',
                'busB': 'uint16',
                'headway':'uint16',
                'busB_ttls':'uint16'
            }
        )[['line','direction','datetime','hw_pos','busA','busB','headway','busB_ttls']]
    elif name == 'series' :
        #Read last series data
        df = pd.read_csv('../Data/RealTime/series.csv',
            dtype={
                'line': 'str'
            }
        )
    elif name =='anomalies' :
        #Read last anomalies data
        df = pd.read_csv('../Data/Anomalies/anomalies.csv',
            dtype={
                'line': 'str'
            }
        )
    return df


def calculate_coords(df,stop_id,dist_to_stop) :
    line_sn = df.iloc[0].line_sn
    direction = str(df.iloc[0].direction)
    bus_distance = int(lines_collected_dict[line_sn][direction]['distances'][str(stop_id)]) - dist_to_stop
    nearest_row = find_nearest_row_by_dist(df,bus_distance)
    return nearest_row.lon, nearest_row.lat


def find_nearest_row_by_dist(df,dist_traveled) :
    min_dist_error = 1000000.0
    df_reduced = df.loc[(df.dist_traveled>dist_traveled-500)&(df.dist_traveled<dist_traveled+500)]
    if df_reduced.shape[0]!=0:
        for row in df_reduced.itertuples() :
            error = abs(row.dist_traveled-dist_traveled)
            if  error < min_dist_error :
                min_dist_error = error
                nearest_row = row
    else :
        nearest_row = df.iloc[0]
    return nearest_row


def calc_map_params(df) :
    #Select line line shapes
    line = df.line.iloc[0]
    line1 = lines_shapes.loc[(lines_shapes.line_sn == line) & (lines_shapes.direction == 1)]
    line2 = lines_shapes.loc[(lines_shapes.line_sn == line) & (lines_shapes.direction == 2)]
    dest1 = lines_collected_dict[line]['destinations'][1]

    lons,lats = [],[]
    for bus in df.itertuples() :
        if bus.destination == dest1 :
            lon,lat = calculate_coords(line1,bus.stop,bus.DistanceBus)
        else :
            lon,lat = calculate_coords(line2,bus.stop,bus.DistanceBus)

        lons.append(lon)
        lats.append(lat)
    
    df['lon'] = lons
    df['lat'] = lats 
    center_x = df.lon.mean()
    center_y = df.lat.mean()

    if df.shape[0] > 1 :
        zoom = min(max(3*math.log(1/(min(math.sqrt((max(lons)-min(lons))**2 + (max(lats)-min(lats))**2),1))),11.5),13.5)
    else :
        zoom = 14
    return df,center_x,center_y,zoom


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

    #We drop the duplicated buses keeping the instance that is closer to a stop
    line_df = line_df.sort_values(by='DistanceBus').drop_duplicates(['bus'],keep='first')
    
    line_df,center_x,center_y,zoom = calc_map_params(line_df)

    #We create the figure object
    new_map = go.Figure()

    #And set the figure layout
    new_map.update_layout(
        title='<b>BUSES POSITION</b>',
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
            zoom=zoom,
            style=style
        )
    )

    #Add the bus points to the figure
    for bus in line_df.itertuples() :
        #Assign color based on bus id
        color = colors[bus.bus%len(colors)]
        #Bus marker
        new_map.add_trace(go.Scattermapbox(
            lat=[bus.lat],
            lon=[bus.lon],
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
    if line_df[line_df.destination == dest1].shape[0] < 1 :
        stop_names = lines_collected_dict[line]['2']['stops'][1:]
        lines_hovered = [line2]
    elif line_df[line_df.destination == dest2].shape[0] < 1 :
        stop_names = lines_collected_dict[line]['1']['stops'][1:]
        lines_hovered = [line1]
    else :
        stop_names = lines_collected_dict[line]['1']['stops'][1:] + lines_collected_dict[line]['2']['stops'][1:]
        lines_hovered = [line1,line2]

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
    for line_shape in lines_hovered :
        color = 'rgb(108, 173, 245)' if line_shape.iloc[0].direction == 1 else 'rgb(243, 109, 90)'
        new_map.add_trace(go.Scattermapbox(
            lat=line_shape.lat,
            lon=line_shape.lon,
            mode='lines',
            line=dict(width=1.5, color=color),
            text='Línea : {}-{}'.format(line,line_shape.iloc[0].direction),
            hoverinfo='skip'
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
        title='<b>HEADWAYS</b> - (Hover buses or links to see more)',
        legend_title='<b>Bus ids</b>',
        xaxis = dict(
            title_text = 'Seconds remaining to destination',
            nticks=20
        ),
        yaxis = dict(
            type='category',
            showgrid=False, 
            zeroline=False
        ),
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
        for i in range(hw1.shape[0]-1):
            N,X = 10,[hw1.iloc[i].busB_ttls,hw1.iloc[i+1].busB_ttls]
            X_new = []
            for k in range(N+1):
                X_new.append(X[0]+(X[1]-X[0])*k/N)
            
            graph.add_trace(go.Scatter(
                x=X_new,
                y=[('<b>'+dest1) for i in range(len(X_new))],
                mode='lines',
                line=dict(width=1.5, color=colors2[(hw1.iloc[i+1].busA+hw1.iloc[i+1].busB)%len(colors2)]),
                showlegend=False,
                hoverinfo='text',
                text='<b>Bus group: ' + str(hw1.iloc[i+1].busA) + '-' + str(hw1.iloc[i+1].busB) + '</b> <br>' + \
                    'Headway: ' + str(hw1.iloc[i+1].headway)+'s'
            ))

    if hw2.shape[0] == 0 :
        max_dist2 = 0
    else :
        max_dist2 = hw2.busB_ttls.max()
        #Add trace
        for i in range(hw2.shape[0]-1):
            N,X = 10,[hw2.iloc[i].busB_ttls,hw2.iloc[i+1].busB_ttls]
            X_new = []
            for k in range(N+1):
                X_new.append(X[0]+(X[1]-X[0])*k/N)
            
            graph.add_trace(go.Scatter(
                x=X_new,
                y=[('<b>'+dest2) for i in range(len(X_new))],
                mode='lines',
                line=dict(width=1.5, color=colors2[(hw2.iloc[i+1].busA+hw2.iloc[i+1].busB)%len(colors2)]),
                showlegend=False,
                hoverinfo='text',
                text='<b>Bus group: ' + str(hw2.iloc[i+1].busA) + '-' + str(hw2.iloc[i+1].busB) + '</b> <br>' + \
                    'Headway: ' + str(hw2.iloc[i+1].headway)+'s'
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
            y=['<b>'+dest],
            marker=dict(
                size=20,
                color=color
            ),
            text=['<b>Bus: ' + str(bus.busB) + '</b> <br>' + str(bus.headway)+'s to next bus <br>' + str(bus.busB_ttls) + 's to last stop'],
            hoverinfo='text'
        ))

    #Finally we return the graph
    return graph


def build_time_series_graph(series_df) :

    graph = go.Figure()

    #Set title and layout
    graph.update_layout(
        title='<b>1D HEADWAYS TIME SERIES</b> - (In seconds)',
        legend_title='<b>Group ids</b>',
        yaxis = dict(
            nticks=20
        ),
        legend = dict(
            x=-0.1,
            y=-0.075,
            orientation='h'
        ),
        margin=dict(r=0, l=0, t=40, b=0),
        hovermode='closest'
    )

    series_df = series_df.loc[series_df.dim == 1]
    if series_df.shape[0] < 1 :
        return graph

    #All bus names
    bus_names_all = ['bus' + str(i) for i in range(1,3)]
    hw_names_all = ['hw' + str(i) + str(i+1) for i in range(1,2)]

    #Min and max datetimes
    min_time = series_df.datetime.min()
    max_time = series_df.datetime.max()

    #Locate unique groups
    unique_groups = []
    unique_groups_df = series_df.drop_duplicates(bus_names_all)
    for i in range(unique_groups_df.shape[0]):
        group = [unique_groups_df.iloc[i][bus_names_all[k]] for k in range(2)]
        unique_groups.append(group)

    for group in unique_groups :
        #Build indexing conditions
        conds = [series_df[bus_names_all[k]] == group[k] for k in range(2)]
        final_cond = True
        for cond in conds :
            final_cond &= cond
        group_df = series_df.loc[final_cond]
        group_df = group_df.sort_values('datetime')
        
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
            y=group_df.hw12,
            mode='lines+markers',
            line=dict(width=1.5,color=colors2[(group_df.bus1.iloc[0]+group_df.bus2.iloc[0])%len(colors2)])
        ))

    return graph


def build_m_dist_graph(series_df,line) :

    graph = go.Figure()

    #Read dict
    while True :
        try :
            with open('../Data/Anomalies/hyperparams.json', 'r') as f:
                hyperparams = json.load(f)
            conf = hyperparams[line]['conf']
            break
        except :
            continue

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
    
    anomalies_df = anomalies_df[['dim','group','anom_size','m_dist','datetime']]

    groups_dfs = []
    for group in anomalies_df.group.unique():
        group_df = anomalies_df[anomalies_df.group == group]
        group_df['m_dist'] = round(group_df.m_dist.mean(),4)
        groups_dfs.append(group_df)

    #Final data for the table
    anomalies_df = pd.concat(groups_dfs)
    anomalies_df = anomalies_df.sort_values('datetime',ascending=False).drop_duplicates('group',keep='first')

    table = dash_table.DataTable(
        id='table',
        filter_action='native',
        sort_action='native',
        page_size= 10,
        style_header={
            'color':'white',
            'backgroundColor': '#6A5ACD'
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

# CALLBACK 0a - New interval loading
@app.callback(
    [Output('new-interval-loading','children')],
    [Input('interval-component','n_intervals'),Input('update-button','n_clicks')]
)
def new_interval(n_intervals,n_clicks) :

    return [html.H1('Loading',style={'display':'none'})]

# CALLBACK 0a - Title and sliders
@app.callback(
    [Output('tab-title','children'),Output('conf','children'),Output('size-th','children')],
    [Input('interval-component','n_intervals'),
    Input('url', 'pathname')]
)
def update_title_sliders(n_intervals,pathname) :
    line = pathname[10:]
    
    with open('../Data/Anomalies/hyperparams.json', 'r') as f:
        hyperparams = json.load(f)
    
    conf = hyperparams[line]['conf']
    size_th = hyperparams[line]['size_th']

    #And return all of them
    return [
        [html.H1('Line {} Real-Time Monitoring'.format(line), className='title is-3')],
        [
            html.Label(
                [
                    "Confidence",
                    dcc.Slider(id='conf-slider',
                        min=90,
                        max=100,
                        step=0.05,
                        marks={i: str(i)+'%' for i in [90+k*1 for k in range(11)]},
                        value=conf*100,
                    )
                ],
            )
        ],
        [
            html.Label(
                [
                    "Size threshold",
                    dcc.Slider(id='size-th-slider',
                        min=1,
                        max=15,
                        marks={i: str(i) for i in range(1,16)},
                        value=size_th,
                    )
                ],
            )
        ]
    ]

# CALLBACK 0b - Sliders update
@app.callback(
    [Output('hidden-div','children')],
    [Input('conf-slider','value'),
    Input('size-th-slider', 'value'),
    Input('url', 'pathname')]
)
def update_hyperparams(conf,size_th,pathname) :
    line = pathname[10:]
    try :
        if (conf == 0) | (size_th == 0) :
            return [html.H1('',className='box subtitle is-6')]

        conf = round(conf/100,3)

        #Read dict
        with open('../Data/Anomalies/hyperparams.json', 'r') as f:
            hyperparams = json.load(f)
        
        #Update hyperparams
        hyperparams[line]['conf'] = conf
        hyperparams[line]['size_th'] = size_th 

        #Write dict
        with open('../Data/Anomalies/hyperparams.json', 'w') as fp:
            json.dump(hyperparams, fp)

    except :
        pass

    return [html.H1('Confidence set to {} and size threshold set to {} in the next update'.format(conf,size_th),className='box subtitle is-6')]

# CALLBACK 1 - Buses Position
@app.callback(
    [
        Output('buses-pos-div','children')
    ],
    [
        Input('interval-component','n_intervals'),
        Input('update-button','n_clicks'),
        Input('url', 'pathname'),
        Input('flat-hws','hoverData')
    ]
)
def update_buses_position(n_intervals,n_clicks,pathname,hoverData) :

    line = pathname[10:]

    try :
        if 'text' in hoverData['points'][0].keys() :
            hover_buses = [int(hoverData['points'][0]['text'].split('<b>Bus: ')[1].split('</b>')[0])]
        else :
            hws_burst = read_df('hws_burst')

            dest = hoverData['points'][0]['y'][3:]
            x = hoverData['points'][0]['x']

            direction = 1 if dest == lines_collected_dict[line]['destinations'][1] else 2

            buses = hws_burst[(hws_burst.line == line) & (hws_burst.direction == direction) & \
                                (hws_burst.busB_ttls >= x)].sort_values('busB_ttls')
            hover_buses = [buses.busA.iloc[0],buses.busB.iloc[0]]

    except :
        hover_buses = None

    burst = read_df('burst')

    #Line dataframe
    line_burst = burst.loc[burst.line == line]
    if hover_buses :
        line_burst = line_burst[line_burst.bus.isin(hover_buses)]
    
    if line_burst.shape[0] < 1 :
        return [
            html.H1('No buses were found inside the line.',className ='title is-5')
        ]
    
    #Create map
    new_map = build_map(line_burst)

    #And return all of them
    return [
        dcc.Graph(
            id = 'map',
            className = 'box',
            style=dict(height='39vh'),
            figure = new_map,
            config={
                'displayModeBar': False
            }
        )
    ]
    

# CALLBACK 2 - Buses headways representation
@app.callback(
    [
        Output('flat-hws-div','children')
    ],
    [
        Input('interval-component','n_intervals'),
        Input('update-button','n_clicks'),
        Input('url', 'pathname')
    ]
)
def update_flat_hws(n_intervals,n_clicks,pathname) :
    line = pathname[10:]

    hws_burst = read_df('hws_burst')

    line_hws = hws_burst.loc[hws_burst.line == line]

    if line_hws.shape[0] < 1 :
        return [
            html.H1('No buses were found inside the line.',className ='title is-5')
        ]

    #Create graph
    flat_hws_graph = build_graph(line_hws)

    graph = dcc.Graph(
        id = 'flat-hws',
        className = 'box',
        style=dict(height='39vh'),
        figure = flat_hws_graph,
        config={
            'displayModeBar': False,
        },
        clear_on_unhover=True
    )

    #And return all of them
    return [graph]


# CALLBACK 3 - 1D Headways Time Series
@app.callback(
    [
        Output('time-series-hws-div','children')
    ],
    [
        Input('interval-component','n_intervals'),
        Input('update-button','n_clicks'),
        Input('url', 'pathname'),
        Input('flat-hws','hoverData')
    ]
)
def update_time_series_hws(n_intervals,n_clicks,pathname,hoverData) :
    line = pathname[10:]

    try :
        if 'text' in hoverData['points'][0].keys() :
            hover_buses = [int(hoverData['points'][0]['text'].split('<b>Bus: ')[1].split('</b>')[0])]
        else :
            hws_burst = read_df('hws_burst')

            dest = hoverData['points'][0]['y'][3:]
            x = hoverData['points'][0]['x']

            direction = 1 if dest == lines_collected_dict[line]['destinations'][1] else 2

            buses = hws_burst[(hws_burst.line == line) & (hws_burst.direction == direction) & \
                                (hws_burst.busB_ttls >= x)].sort_values('busB_ttls')
            hover_buses = [buses.busA.iloc[0],buses.busB.iloc[0]]
    except :
        hover_buses = None

    series = read_df('series')
    
    line_series = series.loc[series.line == line]
    
    if hover_buses :
        if len(hover_buses) == 1 :
            line_series = line_series.loc[(line_series.bus1 == hover_buses[0]) | (line_series.bus2 == hover_buses[0])]
        elif len(hover_buses) == 2 :
            line_series = line_series.loc[(line_series.bus1 == hover_buses[0])]

    if line_series.shape[0] < 1 :
        return [
            html.H1('No headways to analyse. There are less than 2 buses inside each line direction.',className ='title is-5')
        ]

    #Create mh dist graph
    time_series_graph = build_time_series_graph(line_series)
    
    graph = dcc.Graph(
        id = 'time-series-hws',
        className = 'box',
        style=dict(height='39vh'),
        figure = time_series_graph,
        config={
            'displayModeBar': False
        }
    )

    #if n_intervals == 0 :
        #graph = dcc.Loading(type='cube',children = [graph])

    #And return all of them
    return [graph]


# CALLBACK 4 - Mahalanobis Distance series
@app.callback(
    [
        Output('mdist-hws-div','children')
    ],
    [
        Input('interval-component','n_intervals'),
        Input('update-button','n_clicks'),
        Input('url', 'pathname')
    ]
)
def update_mdist_series(n_intervals,n_clicks,pathname) :
    line = pathname[10:]

    series = read_df('series')
    
    line_series = series.loc[series.line == line]

    if line_series.shape[0] < 1 :
        return [
            html.H1('No headways to analyse. There are less than 2 buses inside each line direction.',className ='title is-5')
        ]

    #Create mh dist graph
    m_dist_graph = build_m_dist_graph(line_series,line)

    graph = dcc.Graph(
        id = 'mdist-hws',
        className = 'box',
        style=dict(height='39vh'),
        figure = m_dist_graph,
        config={
            'displayModeBar': False
        }
    )
    
    #if n_intervals == 0 :
        #graph = dcc.Loading(type='cube',children = [graph])

    #And return all of them
    return [graph]


# CALLBACK 5 - Anomalies series
@app.callback(
    [
        Output('anom-hws-div','children')
    ],
    [
        Input('interval-component','n_intervals'),
        Input('update-button','n_clicks'),
        Input('url', 'pathname')
    ]
)
def update_anomalies_table(n_intervals,n_clicks,pathname) :
    line = pathname[10:]

    anomalies = read_df('anomalies')
    
    line_anoms = anomalies.loc[anomalies.line == line]

    #Create anomalies table
    anoms_table = build_anoms_table(line_anoms)

    #And return all of them
    return [
        html.Div(className = 'box', style=dict(height='39vh'), children = [
            html.H2('Detected anomalies',className = 'subtitle is-5'),
            anoms_table
        ])
    ]
    

