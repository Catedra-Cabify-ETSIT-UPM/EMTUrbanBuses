import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash import callback_context as context

import pandas as pd
import json

import plotly.graph_objects as go
import plotly.io as pio

import networkx as nx

import statistics
from statistics import mean
import math

import datetime

from app import app

# WE LOAD THE DATA
stops = pd.read_csv('../Data/Static/stops.csv')

stops_net = nx.read_gpickle("../Data/Static/StopsNetworks/stops_net_graph")
stops_day_net = nx.read_gpickle("../Data/Static/StopsNetworks/stops_day_net_graph")
stops_night_net = nx.read_gpickle("../Data/Static/StopsNetworks/stops_night_net_graph")

lines_shapes = pd.read_csv('../Data/Static/lines_shapes.csv')
with open('../Data/Static/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)

night_lines = [str(i) for i in range(500,600)]
rank_params = ['pagerank','deg_centrality','in_centrality','out_centrality']

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

layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.Div(className='columns', children = [
            html.Div(className='column',children = [
                html.H1('Desired Lines Analysis',className = 'title is-3'),
                html.H2('Blue = From A to B, Red = From B to A',className = 'subtitle is-5'),
            ]),
            html.Div(className='column', children = [
                html.Label(
                    [
                        "Lines",
                        dcc.Dropdown(
                            id="lineIds-select",
                            options=[{"label": i, "value": i} for i in ['Selected','Night-time','Day-time (Slow charging)','All (Slow charging)'] + list(line_stops_dict.keys())],
                            value=['Selected'],
                            searchable=True,
                            multi=True
                        ),
                    ]
                ),
            ]),
            html.Div(className = 'column is-narrow', children = [
                html.Label(
                    [
                        "Order parameter",
                        dcc.Dropdown(
                            id="param-selector",
                            options=[{"label": i, "value": i} for i in rank_params],
                            placeholder='Select top words order param',
                            value=rank_params[0],
                            searchable=True,
                            multi=False,
                        )
                    ]
                )
            ]),
            html.Div(className = 'column is-narrow', children = [
                html.Label(
                    [
                        "Axis type",
                        dcc.Dropdown(
                            id="axis-type",
                            options=[{"label": i, "value": i} for i in ['linear','log']],
                            placeholder='Select axis type',
                            value='linear',
                            searchable=True,
                            multi=False
                        )
                    ]
                )
            ])
        ]),
        html.Div(className='columns', children=[
            html.Div(className='column', style={'position':'relative'}, children=[
                html.Div(id='lines-graph',className='box', children=[
                    dcc.Graph(id='lines-map', figure=go.Figure(), style={'display':'none'}, clear_on_unhover=True, config={'displayModeBar': False})
                ]),
                html.Div(id='hovered-stop-info', className='box', style={'position':'absolute', 'top':'0', 'right':'0', 'z-index':'0'}, children = [
                    dcc.Graph(id='hovered-stop-map', figure=go.Figure(),  config={'displayModeBar': False}, style={'display':'none'})
                ])
            ]),
            html.Div(className='column', style={'position':'relative'}, children=[
                html.Div(className='box', id='stops-net-graph', children=[
                    dcc.Graph(id='net-graph', figure=go.Figure(), style={'display':'none'}, clear_on_unhover=True, config={'displayModeBar': False})
                ]),
                html.Div(id='hovered-stop-graph', className='box', style={'position':'absolute', 'top':'0', 'right':'0', 'z-index':'0'}, children = [
                    dcc.Graph(id='hovered-stop-subgraph', figure=go.Figure(),  config={'displayModeBar': False}, style={'display':'none'})
                ])
            ]),
            html.Div(className='column',children=[
                html.Div(className='box', id='top-stops-graph', children = [
                    dcc.Graph(id='stops-rank-graph', figure=go.Figure(), style={'display':'none'}, clear_on_unhover=True, config={'displayModeBar': False})
                ])
            ])
        ])
    ])
])

#Token and styles for the mapbox api
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style = 'mapbox://styles/alejp1998/ck9voa0bb002y1ipcx8j00oeu'
pio.templates.default = 'plotly_white'


#FUNCTIONS
def build_lines_map(stops_of_lines,stops_selected,lines_selected) :
    #We set the center of the map
    center_x = (stops_selected.lon.max()+stops_selected.lon.min())/2
    center_y = (stops_selected.lat.max()+stops_selected.lat.min())/2

    #Style depending on hour
    now = datetime.datetime.now()

    #We create the figure object
    fig = go.Figure()

    #Add lines to the figure
    Xed,Yed = [],[]
    for line_id in lines_selected.line_id.unique() :
        for direction in [1,2] :
            color = '#1E90FF' if direction == 1 else '#B22222'
            line_dir_df = lines_selected.loc[(lines_selected.line_id == line_id) & (lines_selected.direction == direction)]
            fig.add_trace(go.Scattermapbox(
                lat=line_dir_df.lat,
                lon=line_dir_df.lon,
                mode='lines',
                line=dict(width=2, color=color),
                text='Línea : {}-{}'.format(line_id,direction),
                hoverinfo='text',
                opacity=1
            ))

    #Add the stops to the figure
    fig.add_trace(go.Scattermapbox(
        lat=stops_selected.lat,
        lon=stops_selected.lon,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=10,
            color='#2F4F4F',
            opacity=0.9
        ),
        text=['<b>[' + str(stop.id) + '] ' + stop.stop_name +  '</b>' for stop in stops_selected.itertuples()],
        hoverinfo='text'
    ))

    #And set the figure layout
    fig.update_layout(
        title='<b>LINES AND STOPS ON THE MAP</b> - {} Different stops'.format(len(stops_of_lines)),
        margin=dict(r=0, l=0, t=30, b=0),
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
            zoom=11.7,
            style=style
        )
    )

    return fig


def build_bar_graph(top_stops,selected_param,axis_type) :
    #Sort values
    top_stops = top_stops.sort_values(selected_param,ascending=False).iloc[0:15]

    bar_graph = go.Figure()
    for i in range(top_stops.shape[0]) :
        stop = top_stops.iloc[i]
        bar_graph.add_trace(go.Bar(
            x=['[' + str(stop.name) + '] ' + stop.stop_name],
            y=[stop[selected_param]],
            name=stop.stop_name,
            text='<b>[' + str(stop.name) + '] ' + stop.stop_name +  '</b>'\
              '<br>{}: '.format(selected_param) + str(round(stop[selected_param],6)),
            hoverinfo='text'
        ))

    bar_graph.update_layout(
        title='<b>STOPS RANKING',
        margin=dict(r=0, l=0, t=30, b=0),
        showlegend=False,
        xaxis=dict(
            tickfont=dict(size=10)
        ),
        yaxis=dict(
            title=selected_param,
            type=axis_type
        ),
    )
    return bar_graph

def build_net_graph(lineIds):
    invalid_lines = []
    for line in lineIds:
        if ['1','2'] != list(line_stops_dict[line].keys()):
            invalid_lines.append(line)

    #Initialize network graph
    G=nx.DiGraph()

    #Build nodes
    ld_stops_dict = {}
    for line in lineIds :
        if line in invalid_lines :
            continue
        ld_stops = line_stops_dict[line]['1'] + line_stops_dict[line]['2']

        #Remove stops that arent in the df
        pop_indexes = []
        for i in range(len(ld_stops)) :
            if len(ld_stops) > 0 :
                if stops[stops.id==int(ld_stops[i])].shape[0] < 1 :
                    pop_indexes.append(i)
        for pop_index in pop_indexes[-1::-1] :
            ld_stops.pop(pop_index)

        #Add to dict
        ld_stops_dict[line] = {}
        ld_stops_dict[line] = ld_stops

        for i in range(len(ld_stops)) :
            stop = int(ld_stops[i])
            stop_data = stops[stops.id==stop]
            #Add node to graph if not in
            try :
                node = G.nodes[stop]
            except :
                G.add_node(stop)

            #Add coordinates of node as position
            if stop_data.shape[0] > 0 : 
                stop_data = stop_data.iloc[0]
                G.nodes[stop]['name'] = stop_data.stop_name
                G.nodes[stop]['coords'] = (stop_data.lon,stop_data.lat)

                if line not in invalid_lines :
                    try :
                        G.nodes[stop]['lines'] += [int(line)]
                        G.nodes[stop]['lines'] = list(set(G.nodes[stop]['lines']))
                    except :
                        G.nodes[stop]['lines'] = [int(line)]

    #Build links
    for line in lineIds :
        if line in invalid_lines :
            continue
        ld_stops = ld_stops_dict[line]

        for i in range(len(ld_stops)) :
            stop = int(ld_stops[i])
            stop_bef = int(ld_stops[i-1])

            #Back link
            if stop_bef != stop :
                link_lines = intersect(G.nodes[stop_bef]['lines'], G.nodes[stop]['lines'])
                
                link_lines_good = []
                for line in link_lines :
                    if issubset([str(stop_bef),str(stop)],ld_stops_dict[str(line)]):
                        link_lines_good.append(line)

                G.add_edge(stop_bef, stop, weight=len(link_lines_good), lines=link_lines_good)

    return G

def gen_graph(G):
    N = G.number_of_nodes()
    V = G.number_of_edges()

    #pos=nx.spring_layout(G)
    
    Xv=[G.nodes[k]['coords'][0] for k in G.nodes()]
    Yv=[G.nodes[k]['coords'][1] for k in G.nodes()]
    
    center_x = mean(Xv)
    center_y = mean(Yv)
    
    edge_nodes,Xed,Yed,Wed,Led=[],[],[],[],[]
    for edge in G.edges:
        edge_nodes.append((edge[0],edge[1]))
        Xed.append([G.nodes[edge[0]]['coords'][0],G.nodes[edge[1]]['coords'][0]])
        Yed.append([G.nodes[edge[0]]['coords'][1],G.nodes[edge[1]]['coords'][1]])
        Wed+=[G.edges[edge]['weight']]
        Led+=[G.edges[edge]['lines']]
        
    #Linear color map with steps based on quantiles
    max_weight = max(Wed)
    
    line_traces = []
    for i in range(len(Wed)) :
        Xed_orig,Yed_orig = Xed[i],Yed[i]
        Xed_new,Yed_new = [],[]
        N = 10
        for k in range(N+1):
            Xed_new.append(Xed_orig[0]+(Xed_orig[1]-Xed_orig[0])*k/N)
            Yed_new.append(Yed_orig[0]+(Yed_orig[1]-Yed_orig[0])*k/N)
            
        line_trace=go.Scatter(
            x=Xed_new,
            y=Yed_new,
            mode='lines',
            line=dict(
                width=0.5+(Wed[i]/max_weight)*5,
                color='blue'
            ),
            opacity=0.9,
            hoverinfo='text',
            text='Orig Stop: '+str(edge_nodes[i][0])+'<br>'+ \
                'Dest Stop: '+str(edge_nodes[i][1])+'<br>'+ \
                'Lines: '+str(Led[i])+'<br>'+ \
                'Weight: '+str(Wed[i])+'<br>'
        )
        line_traces.append(line_trace)
    
    trace4=go.Scatter(
        x=Xv,
        y=Yv,
        mode='markers',
        name='net',
        marker=dict(
            symbol='circle-dot',
            size=[G.out_degree(k,weight='weight')*2+5 for k in G.nodes()],
            color=colors[2],
            line=dict(
                color='black',
                width=1
            ),
            opacity=0.9
        ),
        text=['<b>[' + str(node) + '] ' + str(G.nodes[node]['name']) +  '</b>'\
              '<br>Out Degree: ' + str(G.out_degree(node,weight='weight')) + \
              '<br>In Degree: ' + str(G.in_degree(node,weight='weight')) + \
              '<br>Lines: ' + str(G.nodes[node]['lines']) \
              for node in G.nodes()],
        hoverinfo='text'
    )
    layout = go.Layout(
        title="<b>STOPS NETWORK GRAPH",
        showlegend=False,
        margin=dict(r=0, l=0, t=30, b=0),
        xaxis = {
            'showgrid':False,
            'visible':False
        },
        yaxis = {
            'showgrid':False,
            'showline':False,
            'zeroline':False,
            'visible':False
        },
        annotations=[
            dict(
                ax=(Xed[i][0] + Xed[i][1]) / 2,
                ay=(Yed[i][0] + Yed[i][1]) / 2, axref='x', ayref='y',
                x=(Xed[i][1] * 3 + Xed[i][0]) / 4,
                y=(Yed[i][1] * 3 + Yed[i][0]) / 4, xref='x', yref='y',
                showarrow=True,
                arrowhead=4,
                arrowsize=1+(Wed[i]/max_weight)*2,
                arrowwidth=1,
                opacity=1
            ) for i in range(len(Xed))]
    )

    data=line_traces + [trace4]
    graph=go.Figure(data=data, layout=layout)
    
    return graph

def get_subnet_nodes (subnet_lines) :
    subnet_nodes = []
    for line in subnet_lines :
        for direction in line_stops_dict[line].keys() :
            ld_stops = line_stops_dict[line][direction]
            for stop in ld_stops :
                subnet_nodes.append(int(stop))

    return list(set(subnet_nodes))

def intersect(lst1, lst2): 
    return list(set(lst1) & set(lst2))

def issubset(lst1,lst2):
    lst2 = lst2 + [lst2[0]]
    for i in range(len(lst2)-len(lst1)) :
        if lst1 == lst2[i:i+len(lst1)] :
            return True
    return False

def get_neighbors(G,node) :
    node_edges = list(G.edges(node))
    node_neighbors = []
    for edge in node_edges :
        if edge[0] == node :
            node_neighbors.append(edge[1])
        else :
            node_neighbors.append(edge[0])
    return node_neighbors


# CALLBACKS

# CALLBACK 1 - Requested Lines Plotting
@app.callback(Output('lines-graph','children'),
              [Input('lineIds-select','value')])
def update_lines_graph(lineIds):
    try :
        if type(lineIds) is str:
            lineIds = [lineIds]
        
        if 'All (Slow charging)' in lineIds :
            stops_of_lines = stops.id.tolist()
            stops_selected = stops
            lines_selected = lines_shapes
        else:
            if 'Day-time (Slow charging)' in lineIds :
                lineIds = []
                for line in line_stops_dict.keys():
                    if line not in night_lines :
                        lineIds.append(line)
            elif 'Night-time' in lineIds :
                lineIds = []
                for line in line_stops_dict.keys():
                    if line in night_lines :
                        lineIds.append(line)
            elif 'Selected' in lineIds:
                lineIds = ['1','44','82','91','92','99','132','133','502','506']

            stops_of_lines = []
            for lineId in lineIds :
                try :
                    if line_stops_dict[lineId] != None :
                        if line_stops_dict[lineId]['1'] != None :
                            stops_of_lines = stops_of_lines + line_stops_dict[lineId]['1']
                        if line_stops_dict[lineId]['2'] != None :
                            stops_of_lines = stops_of_lines + line_stops_dict[lineId]['2']
                except :
                    continue

            stops_of_lines = list(set(stops_of_lines))
            stops_selected = stops.loc[stops.id.isin(stops_of_lines)]

            lineIds = [int(i) for i in lineIds]
            lines_selected = lines_shapes.loc[lines_shapes.line_id.isin(lineIds)]

        lines_map = build_lines_map(stops_of_lines,stops_selected,lines_selected)

        #And finally we return the graph element
        if len(stops_of_lines)==0 :
            return [
                html.H1('Please select one or multiple line ids from the list',className='subtitle is-3'),
                dcc.Graph(id = 'lines-map',figure = go.Figure(),style={'display':'none','height':'60vh'}, clear_on_unhover=True, config={'displayModeBar': False})
            ]
        else :
            return [
                dcc.Graph(id = 'lines-map',style=dict(height='60vh'), figure = lines_map, clear_on_unhover=True, config={'displayModeBar': False})
            ]
    except :
        #If there is an error we ask for a valid line id
        return [
            html.H1('Please select one or multiple line ids from the list',className='subtitle is-3'),
            dcc.Graph(id = 'lines-map',figure = go.Figure(),style={'display':'none','height':'60vh'}, clear_on_unhover=True, config={'displayModeBar': False})
        ]

# CALLBACK 2 - Requested Lines Net Graph
@app.callback(Output('stops-net-graph','children'),
              [Input('lineIds-select','value')])
def update_lines_graph(lineIds):
    try :
        
        if type(lineIds) is str:
            lineIds = [lineIds]
        
        #Nodes for the graph
        if 'All (Slow charging)' in lineIds :
            G = stops_net
        elif 'Day-time (Slow charging)' in lineIds :
            G = stops_day_net
        elif 'Night-time' in lineIds :
            G = stops_night_net
        else:
            if 'Selected' in lineIds:
                lineIds = ['1','44','82','91','92','99','132','133','502','506']
            G = build_net_graph(lineIds)
    

        #Gen graph
        net_graph = dcc.Graph(id = 'net-graph',style=dict(height='60vh'),figure = gen_graph(G), clear_on_unhover=True, config={'displayModeBar': False})

        return [
            net_graph
        ]
    except: 
        #If there is an error we ask for a valid line id
        return [
            html.H1('Please select one or multiple line ids from the list',className='subtitle is-3'),
            dcc.Graph(id = 'net-graph',figure = go.Figure(),style={'display':'none','height':'60vh'}, clear_on_unhover=True, config={'displayModeBar': False})
        ]


# CALLBACK 3 - Ranked Stops of Graph
@app.callback(Output('top-stops-graph', 'children'),
              [Input('lineIds-select', 'value'),
              Input('param-selector', 'value'),
              Input('axis-type', 'value')])
def update_ranked_stops(lineIds,selected_param,axis_type):
    try :
        if type(lineIds) is str:
            lineIds = [lineIds]
        
        #Nodes for the graph
        if 'All (Slow charging)' in lineIds :
            G = stops_net
            stops_pr = stops.set_index('id')[['stop_name']]
        else :
            if 'Day-time (Slow charging)' in lineIds :
                G = stops_day_net
                lineIds = []
                for line in line_stops_dict.keys():
                    if line not in night_lines :
                        lineIds.append(line)
            elif 'Night-time' in lineIds :
                G = stops_night_net
                lineIds = []
                for line in line_stops_dict.keys():
                    if line in night_lines :
                        lineIds.append(line)
            else:
                if 'Selected' in lineIds:
                    lineIds = ['1','44','82','91','92','99','132','133','502','506']
                G = build_net_graph(lineIds)
                
            stops_of_lines = []
            for lineId in lineIds :
                try :
                    if line_stops_dict[lineId] != None :
                        if line_stops_dict[lineId]['1'] != None :
                            stops_of_lines = stops_of_lines + line_stops_dict[lineId]['1']
                        if line_stops_dict[lineId]['2'] != None :
                            stops_of_lines = stops_of_lines + line_stops_dict[lineId]['2']
                except :
                    continue

            stops_of_lines = list(set(stops_of_lines))
            stops_selected = stops.loc[stops.id.isin(stops_of_lines)]
            stops_pr = stops_selected.set_index('id')[['stop_name']]

        #Rank nodes
        pagerank = pd.Series(nx.pagerank(G, alpha=0.9))
        deg_centrality = pd.Series(nx.degree_centrality(G))
        in_centrality = pd.Series(nx.in_degree_centrality(G))
        out_centrality = pd.Series(nx.out_degree_centrality(G))

        stops_pr['pagerank'] = pagerank
        stops_pr['deg_centrality'] = deg_centrality
        stops_pr['in_centrality'] = in_centrality
        stops_pr['out_centrality'] = out_centrality

        if stops_pr.shape[0] < 1 :
            #If there is an error we ask for a valid line id
            return [
                html.H1('Please select one or multiple line ids from the list',className='subtitle is-3'),
                dcc.Graph(id = 'stops-rank-graph', figure = go.Figure(), style={'display':'none','height':'60vh'}, clear_on_unhover=True, config={'displayModeBar': False})
            ]

        #Gen bar graph
        top_stops_graph = build_bar_graph(stops_pr,selected_param,axis_type)

        return [
            dcc.Graph(id = 'stops-rank-graph',style=dict(height='60vh'),figure = top_stops_graph, clear_on_unhover=True, config={'displayModeBar': False})
        ]

    except:
        #If there is an error we ask for a valid line id
        return [
            html.H1('Please select one or multiple line ids from the list',className='subtitle is-3'),
            dcc.Graph(id = 'stops-rank-graph', figure = go.Figure(), style={'display':'none','height':'60vh'}, clear_on_unhover=True, config={'displayModeBar': False})
        ]


# CALLBACK 4 - Hovered Stop Data and Location 
@app.callback(Output('hovered-stop-info','children'),
              [Input('lineIds-select','value'),
              Input('lines-map','clickData'),Input('net-graph','clickData'),Input('stops-rank-graph','clickData')])
def update_hovered_stop_info(lineIds,clickData1,clickData2,clickData3) :
    #Hover data
    hovered = True
    
    try :
        if context.triggered[0]['prop_id'] == 'lines-map.clickData':
            stop_id = clickData1['points'][0]['text'].split('[')[1].split(']')[0]

        elif context.triggered[0]['prop_id'] == 'net-graph.clickData':
            stop_id = clickData2['points'][0]['text'].split('[')[1].split(']')[0]
        
        elif context.triggered[0]['prop_id'] == 'stops-rank-graph.clickData':
            stop_id = clickData3['points'][0]['label'].split('[')[1].split(']')[0]
        else :
            hovered = False
    except :
        hovered = False



    if hovered :
        #LineIds
        if type(lineIds) is str:
            lineIds = [lineIds]
            
        if 'All (Slow charging)' in lineIds :
            stops_of_lines = stops.id.tolist()
            stops_selected = stops
            lines_selected = lines_shapes
        else:
            if 'Day-time (Slow charging)' in lineIds :
                lineIds = []
                for line in line_stops_dict.keys():
                    if line not in night_lines :
                        lineIds.append(line)
            elif 'Night-time' in lineIds :
                lineIds = []
                for line in line_stops_dict.keys():
                    if line in night_lines :
                        lineIds.append(line)
            elif 'Selected' in lineIds:
                lineIds = ['1','44','82','91','92','99','132','133','502','506']

        #Create map
        stop_map = go.Figure()

        stop = stops[stops.id == int(stop_id)].iloc[0]

        #And set the figure layout
        stop_map.update_layout(
            title='',
            margin=dict(r=0, l=0, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken='pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrYTFhMDN5aTB3a2kzbG52dmV4bXF4b3EifQ.jtFT5GtqWddniEwssyPYKQ',
                bearing=0,
                center=dict(
                    lat=stop.lat,
                    lon=stop.lon
                ),
                pitch=0,
                zoom=14,
                style=style
            )
        )

        #Add lines to the figure
        G = stops_net
        stop_lines = intersect([int(k) for k in lineIds],G.nodes[stop.id]['lines'])

        lines_selected = lines_shapes.loc[lines_shapes.line_id.isin(stop_lines)]
        for line_id in lines_selected.line_id.unique() :
            for direction in [1,2] :
                color = '#1E90FF' if direction == 1 else '#B22222'
                line_dir_df = lines_selected.loc[(lines_selected.line_id == line_id) & (lines_selected.direction == direction)]
                stop_map.add_trace(go.Scattermapbox(
                    lat=line_dir_df.lat,
                    lon=line_dir_df.lon,
                    mode='lines',
                    line=dict(width=2, color=color),
                    text='Línea : {}-{}'.format(line_id,direction),
                    hoverinfo='text',
                    opacity=1
                ))

        #Add the stops to the figure
        stop_map.add_trace(go.Scattermapbox(
            lat=[stop.lat],
            lon=[stop.lon],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=20,
                color='#2F4F4F',
                opacity=0.9
            )
        ))

        return [
            html.Div(style={'width':'30vh'}, children=[
                html.H1('[{}] {}'.format(stop.id,stop.stop_name), className='subtitle is-5'),
                dcc.Graph(id='hovered-stop-map', figure=stop_map, style={'height':'25vh'}, config={'displayModeBar': False})
            ])
        ]
    else :
        return 'Click a stop'

# CALLBACK 5 - Hovered Stop Graph
@app.callback(Output('hovered-stop-graph','children'),
              [Input('lineIds-select','value'),
              Input('lines-map','clickData'),Input('net-graph','clickData'),Input('stops-rank-graph','clickData')])
def update_hovered_stop_info(lineIds,clickData1,clickData2,clickData3) :
    #Hover data
    hovered = True

    try :
        if context.triggered[0]['prop_id'] == 'lines-map.clickData':
            stop_id = clickData1['points'][0]['text'].split('[')[1].split(']')[0]

        elif context.triggered[0]['prop_id'] == 'net-graph.clickData':
            stop_id = clickData2['points'][0]['text'].split('[')[1].split(']')[0]
        
        elif context.triggered[0]['prop_id'] == 'stops-rank-graph.clickData':
            stop_id = clickData3['points'][0]['label'].split('[')[1].split(']')[0]
        else :
            hovered = False
    except :
        hovered = False
    
    if hovered :
        if type(lineIds) is str:
            lineIds = [lineIds]
        
        #Nodes for the graph
        if 'All (Slow charging)' in lineIds :
            G = stops_net
        elif 'Day-time (Slow charging)' in lineIds :
            G = stops_day_net
        elif 'Night-time' in lineIds :
            G = stops_night_net
        else:
            if 'Selected' in lineIds:
                lineIds = ['1','44','82','91','92','99','132','133','502','506']
            G = build_net_graph(lineIds)

        try :
            #Neighbours set
            stop_nodes = get_subnet_nodes([str(k) for k in G.nodes[int(stop_id)]['lines']])

            #Gen subgraph
            G_sub = G.subgraph(stop_nodes)
            stop_subgraph = gen_graph(G_sub)

            stop = stops[stops.id == int(stop_id)].iloc[0]

            stop_subgraph.add_trace(go.Scatter(
                x=[stop.lon],
                y=[stop.lat],
                mode='markers',
                name='net',
                marker=dict(
                    symbol='circle-dot',
                    size=[G.out_degree(int(stop_id),weight='weight')*2+5],
                    color='orange',
                    line=dict(
                        color='black',
                        width=1
                    ),
                    opacity=0.9
                ),
                text=['<b>[' + stop_id + '] ' + str(G.nodes[int(stop_id)]['name']) +  '</b>'\
                    '<br>Out Degree: ' + str(G.out_degree(int(stop_id),weight='weight')) + \
                    '<br>In Degree: ' + str(G.in_degree(int(stop_id),weight='weight')) + \
                    '<br>Lines: ' + str(G.nodes[int(stop_id)]['lines'])],
                hoverinfo='text'
            ))

            stop_subgraph.update_layout(
                title='',
                margin=dict(r=0, l=0, t=0, b=0),
                xaxis = dict(
                    range=(stop.lon-0.005, stop.lon+0.005)
                ),
                yaxis = dict(
                    range=(stop.lat-0.005, stop.lat+0.005)
                )
            )
            return [
                html.Div(style={'width':'30vh'}, children=[
                    html.H1('[{}] {}'.format(stop_id,G.nodes[int(stop_id)]['name']), className='subtitle is-5'),
                    dcc.Graph(id='hovered-stop-subgraph', figure=stop_subgraph, style={'height':'25vh'}, config={'displayModeBar': False})
                ])
            ]
        except :
            return 'Click a stop'
    else :
        return 'Click a stop'