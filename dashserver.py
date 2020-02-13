import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import geopandas as gpd

#We setup the app
app = dash.Dash(__name__)

#We import the layout from another file
from html_layout import index_string
app.index_string = index_string

#SERVER LAYOUT FOR THE INTERACTIVE ELEMENTS
app.layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('Mapa de lineas y paradas EMT',className = 'title is-3'),
        html.Iframe(id='map',srcDoc=open('M6Data/routes_stops.html','r').read(),width='100%',height='600')
    ])

])

#CARGAMOS LOS DATOS

#FUNCTIONS

#CALLBACKS

#START THE SERVER
if __name__ == '__main__' :
    app.run_server(debug=True)
