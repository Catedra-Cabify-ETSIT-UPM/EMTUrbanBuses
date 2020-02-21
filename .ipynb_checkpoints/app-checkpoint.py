import dash

#We setup the app
app = dash.Dash(__name__)
server = app.server
app.config.suppress_callback_exceptions = True