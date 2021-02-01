import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import datetime
import plotly.graph_objects as go
import numpy as np
import s3fs
import json
import requests
import plotly.express as px
import dash_table
import numpy as np
import pandas as pd
import re
ORANGE = '#fec036'
DGREY = '#2b2b2b'
GREY = '#303030'
TESTING=True
if TESTING:
    tags = ['google','netflix']
    colors = {t: px.colors.qualitative.Plotly[i] for i,t in enumerate(tags)}

def load_csv_data():
    if TESTING:
        df = pd.read_csv('s3_data.csv',delimiter=';')
    else:
        df = pd.read_csv('s3a://{}/{}'.format(bucket_name, csv_name),delimiter=';')
    return df
description = "Track Twitter users' sentiments towards entities (keywords, hashtags etc.) and gain insights into cultural and market trends. "
instructions ="Get started by entering the tags you want to track. After a  few seconds of gathering intial data, the engine will display your trends."
font=dict(
    size=14,
    color='#4e6071'
)
fig_layout = dict(
    font=font,
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(
        tickmode = 'array',
        tickvals = [],
        ticktext = []),
    yaxis=dict(
        gridcolor= '#f0f0f0',
    ),
    hovermode='x unified',
    height=350,
    showlegend=False
)
dispatcher_url = 'http://0.0.0.0:6545/api'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets
)
app.title = 'twitter-sentiment'


app.layout = html.Div(
    id = 'master-div',
    children = [
        # html.Div(
        #     id = 'header',
        #     className ='outer-panel',
        #     children=[
        #         html.Center([html.H3('Twitter Sentiment Tracker', style={'color':'white','margin':'0px','padding':'10px'})]),
        #     ]
        # ),
        html.Div(
            id = 'left-column',
            className='outer-panel',
            children=[
                html.Center(
                    html.H1(['Twitter Sentiment',html.Br(),'Tracker']),
                ),
                html.Div(
                style= {'margin-top': '3rem',
                        'display': 'flex',
                        'flex-direction': 'column',
                        'align-items': 'flex-start',
                        'color': 'white'},
                children=[html.Label("")],
                ),
                html.Div(
                    children=[
                        html.B(
                        html.Div(
                        children=[
                            html.P(description, style={'color':'white'}),
                        ],
                        style={'width':'100%',
                               'word-wrap': 'break-word',
                               'white-space' : 'normal'}
                        ),
                        ),
                        html.Br(),
                        html.Div(
                        children=[
                            html.P(instructions, style={'color':'lightgrey'}),
                        ],
                        style={'width':'100%',
                               'word-wrap': 'break-word',
                               'white-space' : 'normal'}
                        ),
                        html.Br(),
                        html.B(
                        html.Label('Moving average:', style={"color":"white",'margin':'3px'}),
                        ),
                        dcc.Slider(
                            id='smoothing-slider',
                            min=1,
                            max=10,
                            step=1,
                            value=1,
                        ),
                        # html.H6('Refresh rate:'),
                        html.B(
                        html.Label('Refresh rate:', style={"color":"white",'margin':'3px'}),
                        ),
                        dcc.Slider(
                            id='refresh-rate',
                            min=1,
                            max=5,
                            step=1,
                            value=1,
                        ),
                        html.Br(),
                        dcc.Checklist(
                            id = 'checklist',
                            options=[
                                {'label': 'Show average', 'value': 'avg'},
                            ],
                            value=[],
                            style={'color':'white'}
                        )
                    ],
                    style={'padding':'30px'}
                ),
                html.Div(
                    children=[
                    ],
                    style={'padding':'30px'}
                )
            ]
        ),
        html.Div(
            id = 'content-column',
            # style={'color': ORANGE},
            children = [
                html.Div(
                    className='upperpanel row',
                    id = 'header',
                    children=[
                        html.Div(
                            style={'display':'inline'},
                            children = [
                                dcc.Input(
                                    placeholder='Enter tags...',
                                    type='text',
                                    value='',
                                    id='tag-input',
                                    # style={'background-color':GREY,'color':'white'}
                                ),
                                html.Button('Track', id='button', style={'color':'#4e6071'}),
                                html.P(id='fake'),
                                dcc.Dropdown(
                                    id='tag-dropdown',
                                    options=[
                                    ],
                                    value=[],
                                    multi=True,
                                    # style={'background-color':GREY,'color':'white'}
                                ),
                                dcc.Store(
                                    id='tag-colors',
                                    data=colors
                                ),
                                dcc.Store(
                                    id='data',
                                    data=load_csv_data().to_dict('records')
                                )
                            ]
                        ),
                    ]
                ),
                html.Div(
                    className='row',
                    children=[
                        html.Div(
                            id = 'sentiment',
                            className = 'panel lineplot',
                            children = [
                                html.H6('Sentiment',style={'margin':'0'}),
                                dcc.Graph(
                                    id = 'sentimentplot'
                                )
                            ]
                        ),
                        html.Div(
                            id = 'comp-sentiment',
                            className = 'panel rateplot',
                            children = [
                                html.H6('Current sentiment',style={'margin':'0'}),
                                dcc.Graph(
                                    id = 'comp-sentimentplot'
                                )
                            ]
                        ),
                    ]),
                html.Div(
                    className='row',
                    children=[
                        html.Div(
                            id = 'count',
                            className = 'panel countplot',
                            children = [
                                html.H6('Number of tweets',style={'margin':'0'}),
                                dcc.Graph(
                                    id = 'countplot'
                                )
                            ]
                        ),
                        html.Div(
                            id = 'comp-count',
                            className = 'panel rateplot',
                            children = [
                                html.H6('Current rate',style={'margin':'0'}),
                                dcc.Graph(
                                    id = 'comp-countplot'
                                )
                            ]
                        ),
                        html.Div(
                            id = 'tweet-samples',
                            className = 'panel rateplot',
                            style={'padding':'0px','width':'26.7%'},
                            children = [
                                html.H6('Tweets',style={'margin':'10px','padding':'0px 0px 20px 10px'}),
                                dash_table.DataTable(
                                    id = 'sample-table',
                                    columns=[{"name": "", "id": "tweet"}],
                                    style_as_list_view=True,
                                    fill_width=True,
                                    row_selectable=False,
                                    style_table={'width': '100%'},
                                    style_cell={
                                        "padding": "0.5rem 2rem",
                                        "fontFamily": "Open Sans",
                                        # "border": "top",
                                        "width" : '100%',
                                        "height" : 'auto',
                                        "whiteSpace" : 'normal',
                                        'textAlign':'left'
                                    },
                                    style_data_conditional=[
                                        {
                                            'if': {'row_index': 'odd'},
                                            'backgroundColor': '#f7f9fc'
                                        }
                                    ],
                                css=[
                                    {"selector": "tr:hover td", "rule": "color: red !important;"},
                                    {"selector": "td", "rule": "border: none !important;"},
                                    {
                                        "selector": ".dash-cell.focused",
                                        "rule": "",
                                    },
                                    # {"selector": "table", "rule": "--accent: #1e2130;"},
                                    # {"selector": "tr", "rule": "background-color: transparent"},
                                ],
                                )
                            ]
                        ),
                    ]
                ),
                dcc.Interval(
                    id='interval-component',
                    interval=5*1000, # in milliseconds
                    n_intervals=0
                ),
                html.P(id='trigger1'),
                html.P(id='trigger2'),
                html.P(id='trigger3'),
                html.P(id='trigger4'),
                html.P(id='trigger5'),
            ]
        )
    ]
)



@app.callback(
    Output('interval-component', 'n_intervals'),
    Input('smoothing-slider', 'value')
)
def update_smoothing(_):
    return 1

@app.callback(
    Output('interval-component', 'n_intervals'),
    Input('checklist', 'value')
)
def update_checklist(_):
    return 1

@app.callback(
    [Output('trigger1','value'),
    Output('trigger2','value'),
    Output('trigger3','value'),
    Output('trigger4','value'),
    Output('trigger5','value'),
    Output('data', 'data')],
    Input('interval-component','n_intervals')
)
def load_data(_):
    df = load_csv_data()
    return None, None, None, None, None, df.to_dict('records')

@app.callback(
    Output('sample-table','data'),
    Input('trigger1','value'),
    State('data','data')
)
def update_tweetsamples(_, data):
    df = pd.DataFrame(data)
    df['timestamp'] = df.timestamp.apply(lambda x: datetime.datetime.strptime(x.split(',')[0][1:],'%Y-%m-%d %H:%M:%S').timestamp())
    df = df.rename({'avgsentiment':'sentiment','timestamp':'time'}, axis=1).sort_values('time')
    df = df.groupby('tag').last().reset_index()[['tag','tweet1','tweet2']]
    df = pd.concat([df[['tag','tweet1']].rename({'tweet1':'tweet'},axis=1),
    df[['tag','tweet2']].rename({'tweet2':'tweet'},axis=1)],axis=0)

    def clean(x):
        s = re.sub(r'\\\w{1,3}','',x).replace("'",'')
        s = re.sub('^b','',s)
        s = re.sub(r'RT @\w*:','',s)
        s = re.sub(r'@\w*','',s)
        return s
    df['tweet'] = df.tweet.apply(clean)
    return df.to_dict('records')

@app.callback(
    Output('sentimentplot','figure'),
    Input('trigger2','value'),
    [State('smoothing-slider','value'),
     State('tag-colors','data'),
     State('checklist','value'),
     State('data','data')]
)
def update_sentimentplot(_, smoothing_value, cmap, chkbx, data):
    df = pd.DataFrame(data)
    df['timestamp'] = df.timestamp.apply(lambda x: datetime.datetime.strptime(x.split(',')[0][1:],'%Y-%m-%d %H:%M:%S').timestamp())
    df = df.rename({'avgsentiment':'sentiment','timestamp':'time'}, axis=1).sort_values(['time','tag'])
    df['smoothed'] = df.groupby('tag').rolling(window=smoothing_value).mean().reset_index(level=[0])['sentiment']

    fig = px.line(data_frame=df,x='time', y='smoothed', color='tag',
                 hover_name="tag", hover_data=['smoothed'],
                    color_discrete_map=cmap)
    if 'avg' in chkbx:
        for key, m in df.groupby('tag').mean()[['sentiment']].iterrows():
            fig.add_shape(type='line',
                        x0=df['time'].min(),
                        y0=m['sentiment'],
                        x1=df['time'].max(),
                        y1=m['sentiment'],
                        line=dict(color=cmap.get(key,None),dash='dot'),
                        xref='x',
                        yref='y'
            )
    fig.update_layout(fig_layout)
    fig.update_xaxes(title_text='',showline=False, zeroline=False,showspikes=True)
    fig.update_yaxes(title_text='',showline=False, gridwidth=2, zeroline=False, showspikes=True)
    fig.update_traces(mode='lines', hovertemplate='%{y:.2f}',)
    fig.update_layout(showlegend=True)
    return fig

@app.callback(
    Output('countplot','figure'),
    Input('trigger3','value'),
    [State('smoothing-slider','value'),
     State('tag-colors','data'),
     State('checklist','value'),
     State('data','data')]
)
def update_countplot(_, smoothing_value, cmap, chkbx, data):
    df = pd.DataFrame(data)
    df['timestamp'] = df.timestamp.apply(lambda x: datetime.datetime.strptime(x.split(',')[0][1:],'%Y-%m-%d %H:%M:%S').timestamp())
    df = df.rename({'avgsentiment':'sentiment','timestamp':'time'}, axis=1).sort_values(['time','tag'])
    df['smoothed'] = df.groupby('tag').rolling(window=smoothing_value).sum().reset_index(level=[0])['notweets']


    fig = px.line(data_frame=df,x='time', y='smoothed', color='tag',
                 hover_name="tag", hover_data=['smoothed'],
                    color_discrete_map=cmap)

    if 'avg' in chkbx:
        for key, m in df.groupby('tag').mean()[['smoothed']].iterrows():
            fig.add_shape(type='line',
                        x0=df['time'].min(),
                        y0=m['smoothed'],
                        x1=df['time'].max(),
                        y1=m['smoothed'],
                        line=dict(color=cmap.get(key,None),dash='dot'),
                        xref='x',
                        yref='y'
            )
    fig.update_layout(fig_layout)
    fig.update_xaxes(title_text='',showline=False, zeroline=False,showspikes=True)
    fig.update_yaxes(title_text='',showline=False, gridwidth=2, zeroline=False, showspikes=True)
    fig.update_traces(mode='lines', hovertemplate='%{y:.2f}',)
    fig.update_layout()
    return fig

@app.callback(
    Output('comp-sentimentplot','figure'),
    Input('trigger4','value'),
    [State('smoothing-slider','value'),
     State('tag-colors','data'),
     State('data', 'data')]
     )
def update_donutsentiment(_, smoothing_value, cmap, data):
    df = pd.DataFrame(data)
    df['timestamp'] = df.timestamp.apply(lambda x: datetime.datetime.strptime(x.split(',')[0][1:],'%Y-%m-%d %H:%M:%S').timestamp())
    df = df.rename({'avgsentiment':'sentiment','timestamp':'time'}, axis=1).sort_values(['time','tag'])
    df = df.groupby('tag').rolling(window=smoothing_value).mean()[['sentiment']].reset_index(level=[0]).groupby('tag').last().reset_index()
    df['sentiment'] = df['sentiment'].round(2)
    df = df.sort_values('tag', ascending=True)
    fig = px.pie(data_frame=df, values='sentiment', names='tag',hole=0.5, color='tag',
                    color_discrete_map=cmap)

    fig.update_traces(textinfo='value',marker={'line':{'width':5,'color':'white'}})
    fig.update_layout(fig_layout)
    fig.update_layout({'hovermode':False})
    return fig

@app.callback(
    Output('comp-countplot','figure'),
    Input('trigger4','value'),
    [State('smoothing-slider','value'),
     State('tag-colors','data'),
     State('data', 'data')]
)
def update_donutcount(_, smoothing_value, cmap, data):
    df = pd.DataFrame(data)
    df['timestamp'] = df.timestamp.apply(lambda x: datetime.datetime.strptime(x.split(',')[0][1:],'%Y-%m-%d %H:%M:%S').timestamp())
    df = df.rename({'avgsentiment':'sentiment','timestamp':'time'}, axis=1).sort_values(['time','tag'])
    df = df.groupby('tag').rolling(window=smoothing_value).sum()[['notweets']].reset_index(level=[0]).groupby('tag').last().reset_index()

    fig = px.pie(data_frame=df, values='notweets', names='tag',hole=0.5, color='tag',
                    color_discrete_map=cmap)
    fig.update_traces(textinfo='value',marker={'line':{'width':5,'color':'white'}})
    fig.update_layout(fig_layout)
    fig.update_layout({'hovermode':False})
    return fig


@app.callback(
    [Output('tag-dropdown','options'),
     Output('tag-dropdown','value'),
     Output('tag-input','value')],
    Input('button','n_clicks'),
    [State('tag-input','value'),
     State('tag-dropdown','options'),
     State('tag-dropdown','value')]
)
def submit_click(_, val_input, opt, val_dropdown):
    val_input = val_input.upper().strip()
    if val_input:
        if not any([val_input == entry['label'] for entry in opt]):
            opt.append({'label':val_input,'value':val_input})
        if not val_input in val_dropdown:
            val_dropdown.append(val_input)
    return opt, val_dropdown,''

def start_stream(tag):
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    data = {'tag': tag, 'track': [tag], 'languages':['en']}
    data = json.dumps(data)
    r = requests.post(dispatcher_url + '/add', data=data, headers=headers)


def stop_stream(tag):
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    data = {'tag': tag}
    data = json.dumps(data)
    r = requests.post(dispatcher_url + '/stop', data=data, headers=headers)

@app.callback(
    [Output('fake','value'),
     Output('tag-colors','data')],
    Input('tag-dropdown','value'),
    State('tag-dropdown','value')
)
def update_streams(tags, values):
    try:
        r = requests.get(dispatcher_url + '/list')
        results = json.loads(r.text)
        to_start = [t for t in tags if not t in results]
        to_stop = [r for r in results if not r in tags]
        for t in to_start:
            start_stream(t)
        for t in to_stop:
            stop_stream(t)
    except:
        pass

    colors = {t.lower(): px.colors.qualitative.Plotly[i] for i,t in enumerate(tags)}
    return None,colors

# app.run_server(debug=True)
app.run_server(debug=True,host='0.0.0.0',port='8080')
