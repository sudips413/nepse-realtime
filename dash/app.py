from dash import Dash, html, dcc, callback, Output, Input, State, dash_table
from plotly import express as px
import plotly.graph_objects as go
import pandas as pd
import time
from plotly.subplots import make_subplots
import numpy as np

df = pd.read_csv('../data/nepsedata.csv')
dfnew = pd.read_csv('../data/nepsetodayRealtime.csv')


app = Dash(__name__)
#get today's date
today = time.strftime("%d/%m/%Y")

app.layout = html.Div([
    
    html.H1(children='NEPSE Analysis', style={'textAlign':'center','width':'40%', 'margin':'30px auto','color':'red'}),
    ### new real time graph
    html.H4(children=f'Real Time Graph {today}', style={'textAlign':'center','width':'40%', 'margin':'20px auto','color':'blue'}),
    dcc.Dropdown(options=[{'label': i, 'value': i} for i in df['company_name'].unique()], value='ACLBSL', id='dropdown-selection-realtime',style={'width':'40%', 'margin':'auto'}),
    html.Div(id='output-realtime',style={'width':'40%', 'margin':'auto'}),
    dcc.Graph(id='graph-content-realtime',style={'width':'80%', 'margin':'auto'}),      
    # dash_table.DataTable(
    #     ##table for real time data
    #     id='table-realtime',
    #     columns=[{"name": i, "id": i} for i in dfnew.columns],
    #     data=df.to_dict('records'),
    #     style_cell={'textAlign': 'center','width':'100px','height':'30px','padding':'10px'},
    #     style_header={
    #         'backgroundColor': 'rgb(230, 230, 230)',
    #         'fontWeight': 'bold'
    #     },
    #     ),

    # interval to update the graph every 3 minutes
    dcc.Interval(
        id='interval-component',
        interval=60*1000, # in milliseconds
        n_intervals=0
    ),
    
    ##combined graph
    html.H4(children='Combined Graph', style={'textAlign':'center','width':'40%', 'margin':'20px auto','color':'blue'}),
    dcc.Dropdown(options=[{'label': i, 'value': i} for i in df['company_name'].unique()], value='ACLBSL', id='dropdown-selection-combined',style={'width':'40%', 'margin':'auto'}),
    dcc.Graph(id='graph-content-combined',style={'width':'70%', 'display':'flex','align-items':'center','justify-content':'center','margin':'auto'}),
    
    ###new candlestick graph
    html.H4(children='Candlestick Graph', style={'textAlign':'center','width':'40%', 'margin':'20px auto','color':'blue'}),
    dcc.Dropdown(options=[{'label': i, 'value': i} for i in df['company_name'].unique()], value='ACLBSL', id='dropdown-selection-candlestick',style={'width':'40%', 'margin':'auto'}),
    dcc.Checklist(
        id='toggle-rangeslider',
        options=[{'label': 'Include Rangeslider', 
                  'value': 'slider'}],
        value=['slider'],
        style={'margin':'auto','display':'flex','justify-content':'center','align-items':'center','width':'40%','margin-bottom':'20px','margin-top':'20px','padding':'10px'},
    ),
    dcc.Graph(id='graph-content-candlestick',style={'width':'80%', 'margin':'auto'}),
    ## new line graph
    html.H4(children='Line Graph', style={'textAlign':'center','width':'40%', 'margin':'20px auto','color':'blue'}),
    dcc.Dropdown(options=[{'label': i, 'value': i} for i in df['company_name'].unique()], value='ACLBSL', id='dropdown-selection',style={'width':'40%', 'margin':'auto'}),
    dcc.Graph(id='graph-content',style={'width':'80%', 'margin':'auto'}), 
    
    
])




@callback(
    Output('graph-content-realtime', 'figure'),
    Input('dropdown-selection-realtime', 'value'),
    Input('graph-content-realtime', 'relayoutData'),
    Input('interval-component', 'n_intervals')
)
def update_graph_realtime(value, relayoutData,n_intervals):
    dfreal=pd.read_csv('../data/nepsetodayRealtime.csv')
    dff = dfreal[dfreal['company_name']==value]
    today= time.strftime("%m/%d/%Y")
    dff = dff[dff['date']==today]
    
    fig = px.line(dff, x='time', y='LTP', title="Share "+value,hover_data=['LTP'],hover_name='LTP',render_mode='svg',markers=True)
    

    return fig
@callback(
    Output('graph-content-combined', 'figure'),
    Input('dropdown-selection-combined', 'value'),

)
def show_comined_graph(value):
    df= pd.read_csv('../data/nepsedata.csv')
    ##arrange by date in descending order
    df=df.sort_values(by='date',ascending=True)
    # company_name,date,confidence,open_price,lowest_price,highest_price,closing_price,VWAP,total_traded_quantity,Previous_closing,total_traded_value,total_trades,difference,range,difference_percentage,range_percentage,VWAP_percentage,year_high,year_low
    df=df[df['company_name']==value]
    df['ma'] = df['closing_price'].rolling(25).mean()
    df['sigma'] = df['closing_price'].rolling(25).std()
    df['bb_high'] = df['ma'] + df['sigma'] * 2
    df['bb_low'] = df['ma'] - df['sigma'] * 2
    fig = make_subplots(rows=3, cols=1, 
                        # vertical_spacing=0.1, 
                        #the vertical spacing between first and second graph to be 0.1 and second and third graph to be 0.02
                        
                        row_heights=[0.6,0.20,0.20],
                        shared_xaxes=False,
                        specs=[[{"secondary_y": True}],[{"secondary_y": False}],[{"secondary_y": False}]])

    fig.add_trace(
        go.Histogram(
            x=df['total_traded_quantity'],
            y=df['closing_price'],
            orientation='h',
            name='volume hist',
            nbinsx=len(df['total_traded_quantity']),
            nbinsy=len(df['closing_price']),
            hovertemplate=[
                'volume: %{x}<br>',
                'price: %{y}<br>',
            ],
            opacity=0.4,
            marker={
                'color':'steelblue'
            },
    ), secondary_y=True )

    fig.add_trace(
        go.Candlestick(
            x=df['date'],
            open=df['open_price'],
            high=df['highest_price'],
            low=df['lowest_price'],
            close=df['closing_price'],
            name='Candles',
            increasing_line_color='#0ebc6e', 
            decreasing_line_color='#e8482c',
            line={'width': 1},
            hoverlabel={
                'font':{
                    'color':'white',
                    'family':'Open Sans',
                    'size':15
                }
            },
    ), secondary_y=True)

    fig.add_trace(
        go.Scatter(
            x=df.date, 
            y=df.bb_high,
            name='bollinger_high',
            line={'color':'orange','width':1},
            hovertemplate=[],
    ), secondary_y=False)

    fig.add_trace(
        go.Scatter(
            x=df.date,
            y=df.bb_low,
            name='bollinger_low',
            line={'color':'orange','width':1},
            hovertemplate=[],
    ), secondary_y=False)

    fig.add_trace(
        go.Bar(
            x=df.date,
            y=df.total_traded_quantity,
            name='Volume',
            marker_color='steelblue',
    ), secondary_y=False, row=2, col=1)

    fig.add_trace(
        go.Scatter(
            x=df.date,
            y=df.closing_price.ewm(26).mean(),
            name='EMA',
            line={'color':'steelblue','width':2}
    ), secondary_y=False, row=3, col=1)


    fig.update_layout(xaxis2={'anchor': 'y', 'overlaying': 'x', 'side': 'top', 'autorange':'reversed'})
    fig.data[0].update(xaxis='x2')
    fig.update_layout()
    fig.data[4].update(xaxis='x')
    fig.data[5].update(xaxis='x')
    fig.update_xaxes(rangeslider_visible=False, showticklabels=True, row=1, col=1)
    fig.update_layout(autosize=False, width=1000, height=1000)
    return fig
    

@callback(
    Output('graph-content-candlestick', 'figure'),
    Input('dropdown-selection-candlestick', 'value'),
    Input('toggle-rangeslider', 'value')
)
def update_graph_candlestick(value, slider_value):
    dff = df[df['company_name']==value]
    #convert date to day month year
    dff['date'] = pd.to_datetime(dff['date'])
    fig = go.Figure(go.Candlestick(
        x=dff['date'],
        open=dff['open_price'],
        high=dff['highest_price'],
        low=dff['lowest_price'],
        close=dff['closing_price']
    ))
    fig.update_layout(
        plot_bgcolor='white',
        font_color='black',
        xaxis_rangeslider_visible='slider' in slider_value
    )


    return fig

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df[df['company_name']==value]
    #convert date to day month year
    dff['date'] = pd.to_datetime(dff['date'])
    ##fill the background color blue below the line
    fig = px.area(dff, x='date', y='closing_price', title="Share "+value)
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color='black'
    )
    return fig



if __name__ == '__main__':
    app.run_server(debug=True)
