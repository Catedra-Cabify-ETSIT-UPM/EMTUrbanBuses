import dash_core_components as dcc
import dash_html_components as html

from app import app

layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('DASH VISUALIZATION SERVER FOR EMT BUSES',className = 'subtitle is-3'),
        html.H1('By: Alejandro Jarabo Peñas',className = 'subtitle is-4'),
        html.H1('Cátedra Cabify - ETSIT UPM - 2020',className = 'subtitle is-4')
    ])
    
])