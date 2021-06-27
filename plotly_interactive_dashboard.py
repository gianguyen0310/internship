#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import seaborn as sns
import numpy as np
import sqlalchemy
import plotly.express as px
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from jupyter_dash import JupyterDash
from dash.dependencies import Input, Output
from sqlalchemy import create_engine


# In[3]:


# Follows django database settings format, replace with your own settings
DATABASES = {
    'hoss_db':{
        'NAME': '...',
        'USER': '...',
        'PASSWORD': '...',
        'HOST': '...',
        'PORT': ...,
    },
}

# Choose the database to use
db = DATABASES['hoss_db']

# Construct an engine connection string
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user = db['USER'],
    password = db['PASSWORD'],
    host = db['HOST'],
    port = db['PORT'],
    database = db['NAME'],
)

# Create sqlalchemy engine
engine = create_engine(engine_string)

# SQL query
query= '''SELECT join_account.Account_ID AS Account_ID, Account_UUID, Account_Name, Account_Email, join_account.Created_at, Plan_ID, Plan_Name, Date, Requests
               FROM
                      (SELECT accounts.id AS Account_ID,
                              accounts.uuid AS Account_UUID,
                              account_versions.name AS Account_Name,
                              accounts.created_at AS Created_at
                       FROM accounts
                       JOIN ( SELECT DISTINCT ON (account_id) account_id, name
                              FROM account_versions
                              ORDER BY account_id, id DESC
                        ) account_versions
                        ON accounts.id = account_versions.account_id
                      ) AS join_account
                  JOIN
                      (SELECT DISTINCT ON (account_id) account_id, id,
                              user_versions.email AS Account_Email
                       FROM users
                       JOIN ( SELECT DISTINCT ON (user_id) user_id, email
                              FROM user_versions
                              ORDER BY user_id, id DESC
                        ) user_versions
                       ON users.id = user_versions.user_id
                       ORDER BY account_id, id
                      ) AS join_user
                        ON join_account.Account_ID = join_user.account_id
                  JOIN
                      (SELECT plan_versions.plan_id AS Plan_ID,
                              plan_versions.name AS Plan_Name,
                              subscriptions.account_id
                       FROM plans
                       JOIN (SELECT DISTINCT ON (plan_id) plan_id, name
                             FROM plan_versions
                             ORDER BY plan_id, id DESC
                       ) plan_versions
                       ON plans.id = plan_versions.plan_id
                       JOIN (SELECT DISTINCT ON (account_id) account_id, plan_id
                             FROM subscriptions
                             ORDER BY account_id, plan_id DESC
                           ) subscriptions
                       ON plans.id = subscriptions.plan_id
                      ) AS join_plan_sub
                        ON join_account.Account_ID = join_plan_sub.account_id
                  JOIN
                      report_daily_usage
                      ON join_account.Account_ID = report_daily_usage.account_id
            ORDER BY account_id, date DESC
            '''

# Read a table from database into pandas dataframe
df = pd.read_sql_query(query, engine)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%m/%d/%y')
df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%m/%d/%y')
df


# In[ ]:


# Filter necessary columns
df1= df[['account_id', 'account_name','created_at', 'plan_name', 'date','requests']]

# Replace NA values in "account_name" with its "NA" string
pd.options.mode.chained_assignment = None #Turn off warning
df1._update_inplace=df1['account_name'].fillna('NA', inplace= True)

# Pivot the df1 dataframe to get a desired format
daily_request= df1.pivot_table(index=['account_id','account_name','created_at','plan_name'], 
                               columns=['date'], values=['requests'])

# Sort the table by descending requests 
daily_request['total_request']= daily_request.sum(axis= 1)
daily_request_sorted= daily_request.sort_values(by= ['total_request'], ascending= False)

# Create df3 for the Dash bar chart, pie chart
df3= daily_request_sorted.reset_index()
df3['account_id_name']= df3['account_id'].astype(str).str.cat(df3['account_name'], sep = '_') # Create new column combines account_id and account_name
df3.insert(2, 'account_id_name', df3['account_id_name'], allow_duplicates=True) # Move column account_id_name to the third location
df3 = df3.loc[:,~df3.columns.duplicated()] 
df3['Jan_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('01')].sum(axis= 1) # Calculate total request for each month
df3['Feb_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('02')].sum(axis= 1) # need to use get_level_values cause we have multi index
df3['Mar_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('03')].sum(axis= 1)
df3['Apr_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('04')].sum(axis= 1)
df3['May_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('05')].sum(axis= 1)
df3['Jun_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('06')].sum(axis= 1)
df3['Jul_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('07')].sum(axis= 1)
df3['Aug_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('08')].sum(axis= 1)
df3['Sep_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('09')].sum(axis= 1)
df3['Oct_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('10')].sum(axis= 1)
df3['Nov_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('11')].sum(axis= 1)
df3['Dec_request'] = df3.loc[:, df3.columns.get_level_values(1).str.startswith('12')].sum(axis= 1)
df3.drop(df3.filter(regex= '1|2|3|4|5|6|7|8|9|10|11|12' , axis= 1), axis=1, inplace= True) # Drop daily request

# Create df4 for the Dash data table
df4 = pd.DataFrame(daily_request_sorted.to_records())
df4.columns = [hdr.replace("('requests', ", "").replace(")", "").replace("(","").replace("'total_request',","total_request").replace("'","").replace(" ","")                      for hdr in df4.columns]
df4['total_request']= df4['total_request'].astype('int64')
df4['top_n'] = df4['total_request'].rank(method ='average', ascending= False, na_option='keep') 
df4['top_n']= df4['top_n'].astype('int64')
df4.insert(0, 'top_n', df4['top_n'], allow_duplicates=True) # Move column 'rank' to the third location
df4 = df4.loc[:,~df4.columns.duplicated()]


# In[ ]:


# Create the pie chart
pie = px.pie(df3, values= df3['plan_name'].value_counts(), 
             names= df3['plan_name'].value_counts().index.to_list(),
             labels={'names': 'account_plan'},
             title='Proportion of account_plan')
pie.update_traces(textposition='outside', textfont_size=13,
                 texttemplate = '"%{label}": %{value} <br>(%{percent})')
pie.update_layout(legend_title_text='account_plan', legend={'traceorder':'reversed'})

# Create the 'No. of account created by month' bar chart
bar1 = px.bar(df3, x= pd.to_datetime(df3['created_at']).dt.strftime('%m/%y').sort_values(ascending= True).unique().tolist(),
               y= df3.groupby(pd.to_datetime(df3['created_at']).dt.strftime('%m/%y')).size().sort_index(ascending= True))
bar1.update_layout(
    title='Number of account created each month',
    xaxis_title= 'Month',
    yaxis_title= 'No. of account created',
    xaxis = dict(tickangle= -45),
    yaxis = dict(showgrid= True, gridwidth= 1)
    )
bar1.update_xaxes(type='category')
bar1.update_traces(texttemplate='%{value:.2}', textposition='outside')

# Define function to format the data table
def discrete_background_color_bins(df4, n_bins=5, columns= 'all'):
    import colorlover
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
    if columns == 'all':
        if 'id' in df4:
            df4_numeric_columns = df4.select_dtypes('float64').drop(['id'], axis=1)
        else:
            df4_numeric_columns = df4.select_dtypes('float64')
    else:
        df4_numeric_columns = df4[columns]
    df4_max = df4_numeric_columns.max().max()
    df4_min = df4_numeric_columns.min().min()
    ranges = [
        ((df4_max - df4_min) * i) + df4_min
        for i in bounds
    ]
    styles = []
    legend = []
    for i in range(1, len(bounds)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        backgroundColor = colorlover.scales[str(n_bins)]['seq']['Greens'][i - 1]
        color = 'white' if i > len(bounds) / 2. else 'inherit'

        for column in df4_numeric_columns:
            styles.append({
                'if': {
                    'filter_query': (
                        '{{{column}}} >= {min_bound}' +
                        (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                    ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                    'column_id': column
                },
                'backgroundColor': backgroundColor,
                'color': color
            })
        legend.append(
            html.Div(style={'display': 'inline-block', 'width': '60px'}, children=[
                html.Div(
                    style={
                        'backgroundColor': backgroundColor,
                        'borderLeft': '1px rgb(50, 50, 50) solid',
                        'height': '10px'
                    }
                ),
                html.Small(round(min_bound, 2), style={'paddingLeft': '2px'})
            ])
        )

    return (styles, html.Div(legend, style={'padding': '5px 0 5px 0'}))
(styles, legend) = discrete_background_color_bins(df4)

# Create DASH app
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    
    html.Div([
    html.H3(children='Daily Report Dashboard')
            ]),
    html.Br(),
    
    html.Div([
    html.Label('Number of daily request sorted by descending "total_request"', style={'fontSize':18}),
    html.Div(legend, style={'float': 'right'}),
    dash_table.DataTable(
        data=df4.to_dict('records'),
        sort_action='native',
        page_size= 15,
        columns=[{'name': i, 'id': i} for i in df4.columns],
        fixed_columns={'headers': True, 'data': 5},
        style_table={'minWidth': '100%'},
        style_cell={'minWidth': '100%'},
        style_data_conditional=styles,
        style_header={
        'backgroundColor': 'rgb(230, 230, 230)'},
        style_cell_conditional=[
        {
            'if': {'column_id': i},
            'textAlign': 'left'
        } for i in ['top_n','account_id', 'account_name','created_at','plan_name']
                                ],
                        ),
            ]),
    html.Br(),
    html.Br(),
    html.Br(),
    
    html.Div([
    html.Label('Select a month'),
    dcc.Dropdown(id= 'choose_month',
    options=[
        {'label': 'All', 'value': 'total_request'},
        {'label': 'January', 'value': 'Jan_request'},
        {'label': 'Febuary', 'value': 'Feb_request'},
        {'label': 'March', 'value': 'Mar_request'},
        {'label': 'April', 'value': 'Apr_request'},
        {'label': 'May', 'value': 'May_request'},
        {'label': 'June', 'value': 'Jun_request'},
        {'label': 'July', 'value': 'Jul_request'},
        {'label': 'August', 'value': 'Aug_request'},
        {'label': 'September', 'value': 'Sep_request'},
        {'label': 'October', 'value': 'Oct_request'},
        {'label': 'November', 'value': 'Nov_request'},
        {'label': 'December', 'value': 'Dec_request'}
    ],
    value= 'total_request',
    multi= False,
    clearable= False,
    style= {'width':'30%'}
    ),
    
    dcc.Graph(id='bar_chart')
             ]),
    html.Br(),
     
    html.Div([
    dcc.Graph(
        id='pie',
        figure= pie),
    
    dcc.Graph(
        id='bar1',
        figure= bar1)
            ], style= { 'columnCount': '2'})
])


@app.callback(
    Output(component_id='bar_chart', component_property='figure'),
    [Input(component_id='choose_month', component_property='value')]
)
def update_barchart(select_month):
    new_df= df3
    barchart= px.bar(new_df, x= new_df[select_month],
                     y= new_df['account_id_name'],
                     color= new_df['plan_name'],
                     color_discrete_map={'Free':'#636EFA',
                                         'Hoss Plus':'#EF553B'},
                     height= (len(df3)) * 20,
                     orientation= 'h',
                     text= new_df[select_month])\
                     .update_yaxes(categoryorder="total ascending")
  
    barchart.update_layout(title='Total requests by account each month',
                           legend_title= 'account_plan', showlegend= True,
                           xaxis_title= 'No. of requests',
                           yaxis_title= 'account_id_name',
                           yaxis = dict(tickmode = 'auto'),
                           xaxis = dict(showgrid= True, gridwidth= 1, nticks= 10))
    
    barchart.update_traces(texttemplate='%{text:.2s}', textposition='outside')
      
    return barchart

if __name__ == '__main__':
    app.run_server(debug=False, port= 5000) #Change port= 8000, 9000, etc. in case you encounter [Errno 48] Address already in use
                                            #Change debug=True to automatically reload dashboard (doesn't work with Jupyter)


# In[ ]:





# In[ ]:




