from click import option
from dash import html, dcc

from app import app

app.layout = html.Div([
    html.Div('test'),
], style={'height': '100%'})

if __name__ == '__main__':
    app.run_server(debug=True, port='8050', dev_tools_ui=True)