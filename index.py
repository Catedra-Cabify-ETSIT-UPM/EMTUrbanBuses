import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from app import app
from apps import app_home, app1, app_credits

#APP INDEX STRING
app.index_string = '''
<!DOCTYPE html>
<html class = "">

    <head>
        {%metas%}
        <title>EMT BUSES</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bulma/0.8.0/css/bulma.min.css">
        <script defer src="https://use.fontawesome.com/releases/v5.3.1/js/all.js"></script>
        <link href='https://fonts.googleapis.com/css?family=Megrim' rel='stylesheet'>
            <style>
                .logo1 {
                    color : white;
                    font-family: 'Megrim';
                    font-size: 30px;
                }
                .navlogo {
                    background: linear-gradient(to left, #4f30a0, #9e2de2);
                }
            </style>
        {%favicon%}
        {%css%}
    </head>

    <body class="body">

        <nav class="navbar navlogo" role="navigation" aria-label="main navigation">
          <div class="navbar-brand">
            <p class="navbar-item logo1">EMT BUSES</p>
          </div>
        
          <div id="navMenu" class="navbar-menu">

            <div class="navbar-start">
              <a class="navbar-item" href="/">MAP</a>
              <a class="navbar-item" href="/apps/app1">LIVE BUSES OF LINE</a>
              <a class="navbar-item" href="/credits">Credits</a>
            </div>
          </div>
        </nav>
        
        <script type="text/javascript">
          (function() {
            var burger = document.querySelector('.burger');
            var nav = document.querySelector('#'+burger.dataset.target);
            burger.addEventListener('click', function(){
              burger.classList.toggle('is-active');
              nav.classList.toggle('is-active');
            });
          })();
        </script>

        <section class="hero"
            {%app_entry%}
        </section>

        <footer class="footer hero has-text-centered">
                <a class='subtitle is-4 is-primary' href="https://github.com/Catedra-Cabify-ETSIT-UPM">CÁTEDRA CABIFY ETSIT UPM</a>
                {%config%}
                {%scripts%}
                {%renderer%}
        </footer>

    </body>

</html>
'''

#APP LAYOUT DIV
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

#CHANGE PAGE DEPENDING ON PATHNAME
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/':
         return app_home.layout
    if pathname == '/apps/app1':
         return app1.layout
    else:
        return app_credits.layout

#START THE SERVER
if __name__ == '__main__':
    app.run_server(debug=True)