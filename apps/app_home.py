import dash_core_components as dcc
import dash_html_components as html

layout = html.Div(className = '', children = [

    html.Div(className = 'box', children = [
        html.H1('MAP OF STOPS AND ROUTES',className = 'title is-3'),
        html.Iframe(id='map',srcDoc=open('M6Data/routes_stops.html','r').read(),width='100%',height='600')
    ])
    
])