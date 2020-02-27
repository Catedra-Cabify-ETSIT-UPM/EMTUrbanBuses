import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import pandas as pd
import geopandas as gpd
import json

import plotly.graph_objects as go
from shapely.geometry import shape
from shapely.geometry import Point, LineString
from shapely import wkt
import fiona
import branca.colormap as cm

import datetime
from collections import Counter

from app import app

# CARGAMOS LOS DATOS
stops = gpd.read_file('M6Data/stops.json')
route_lines = gpd.read_file('M6Data/route_lines.json')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)
    
layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('Desired Lines Map',className = 'title is-3'),
        html.H2('Blue = From A to B, Red = From B to A',className = 'subtitle is-5'),
        html.H3('To plot everything, select "All"',className = 'subtitle is-6'),
        html.Span('Add line Ids: ', className = 'tag is-light is-large'),
        dcc.Dropdown(
            id="lineIds-select",
            options=[{"label": i, "value": i} for i in list(line_stops_dict.keys()) + ['All']],
            value='1',
            searchable=True,
            multi=True
        ),
        html.Div(className='box',id='lines-graph')
    ])
])

#Token and styles for the mapbox api
mapbox_access_token = 'pk.eyJ1IjoiYWxlanAxOTk4IiwiYSI6ImNrNnFwMmM0dDE2OHYzZXFwazZiZTdmbGcifQ.k5qPtvMgar7i9cbQx1fP0w'
style_day = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'
style_night = 'mapbox://styles/alejp1998/ck6z9mohb25ni1iod4sqvqa0d'

# CALLBACKS

# CALLBACK 1 - Requested Lines Plotting
@app.callback(Output(component_id = 'lines-graph',component_property = 'children'),
              [Input(component_id = 'lineIds-select',component_property = 'value')])
def update_lines_graph(lineIds_value):
    '''
    Function that returns a graph with the lines requested and its stops

        Parameters
        ---
        input_lineIds_value: string
            The lines whose stops and trayetories we are going to plot
    '''
    try:
        lineIds = lineIds_value
        showAll = False
        if type(lineIds) is list:
            if 'All' in lineIds :
                showAll = True
        else :
            if lineIds=='All' :
                showAll = True
        
        if showAll :
            stops_of_lines = stops['stop_code'].tolist()
            stops_selected = stops
            lines_selected = route_lines       
        else: 
            stops_of_lines = []
            for lineId in lineIds :
                if line_stops_dict[lineId] != None :
                    if line_stops_dict[lineId]['1'] != None :
                        stops_of_lines = stops_of_lines + line_stops_dict[lineId]['1']['stops']
                    if line_stops_dict[lineId]['2'] != None :
                        stops_of_lines = stops_of_lines + line_stops_dict[lineId]['2']['stops']

            stops_of_lines = list(set(stops_of_lines))
            stops_selected = stops.loc[stops['stop_code'].isin(stops_of_lines)]

            if type(lineIds) is list:
                lines_selected = route_lines.loc[route_lines['line_id'].isin(lineIds)]
            else :
                lines_selected = route_lines.loc[route_lines['line_id']==lineIds]
        
        #We set the center of the map
        center_x = lines_selected.centroid.x.mean()
        center_y = lines_selected.centroid.y.mean()
        #Style depending on hour
        now = datetime.datetime.now()
        if (datetime.time(6,0,0) <= now.time() <= datetime.time(23,30,0)) :
            style = style_day
        else :
            style = style_night
        
        #We create the figure object
        fig = go.Figure()
        #Add the stops to the figure
        fig.add_trace(go.Scattermapbox(
            lat=stops_selected['geometry'].y,
            lon=stops_selected['geometry'].x,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=10,
                color='green',
                opacity=0.7
            ),
            text=stops_selected['stop_code'],
            hoverinfo='text'
        ))
        #Add lines to the figure
        for index, row in lines_selected.iterrows():
            line = row['geometry']
            x_coords = []
            y_coords = []
            for coords in list(line.coords) :
                x_coords.append(coords[0])
                y_coords.append(coords[1])
            
            if row['direction'] == '1' :
                color = 'blue'
            else :
                color = 'red'
            fig.add_trace(go.Scattermapbox(
                lat=y_coords,
                lon=x_coords,
                mode='lines',
                line=dict(width=2, color=color),
                text='Línea : {}-{}'.format(row['line_id'],row['direction']),
                hoverinfo='text'
            ))
        #And set the figure layout
        fig.update_layout(
            title='LINES AND STOPS SELECTED MAP',
            height=500,
            margin=dict(r=0, l=0, t=0, b=0),
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
                zoom=13,
                style=style
            )
        )
        
        #And finally we return the graph element
        if len(stops_of_lines)==0 :
            return 'Please select one or multiple line ids from the list'
        else :
            return [
                html.H2(
                    'Selected lines map',
                    className = 'subtitle is-4'
                ),
                dcc.Graph(
                    id = 'graph',
                    figure = fig
                ),
                html.H2(
                    'Number of different stops involved : {}'.format(len(stops_of_lines)),
                    className = 'subtitle is-5'
                )
            ]
    except :
        #If there is an error we ask for a valid line id
        return 'Please select one or multiple line ids from the list'
        
        
        
        
            
