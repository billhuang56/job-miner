# -*- coding: utf-8 -*-
import os
import flask
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from elasticsearch import Elasticsearch
import requests
import boto3
import pickle
import json
import config
# AWS Config
ACCESS_KEY = config.AWS_ACCESS_KEY_ID #os.environ.get("AWS_ACCESS_KEY_ID")
SECRET_KEY = config.AWS_SECRET_ACCESS_KEY #os.environ.get("AWS_SECRET_ACCESS_KEY")
# Elasticsearch Config
es_host = '10.0.0.13'
es = Elasticsearch([{'host': es_host, 'port': 9200}])
r = requests.get('http://{}:9200'.format(es_host))

server = flask.Flask(__name__)
app = dash.Dash(name='JD App', server=server, external_stylesheets=[dbc.themes.BOOTSTRAP,
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css'])

app.scripts.config.serve_locally=True
app.css.config.serve_locally=True
app.config.suppress_callback_exceptions = True

# Elasticsearch Query
def new_query(state, tags):
    tags = [t.lower() for t in tags]
    body = {"size" : 25,
        'query':{
            'bool':{
                'filter':[{
                    'match':{
                        'state':state
                    }
                }],
                'must':[{
                    'match':{
                        'tags': ' '.join(tags)
                    }
                }],
            }
        }
    }
    return body

def make_new_query(state='NY', tags=[]):
    if r.status_code == 200:
        results = es.search(index='uniq',body=new_query(state, tags))
        return results['hits']['hits']
    return

def recs_query(state, keywords):
    body = {"size" : 5,
        'query':{
            'bool':{
                'filter':[{
                    'match':{
                        'state':state
                    }
                }],
                'should':[{
                    "match":{
                        "keywords": ' '.join(keywords)
                    }
                }],
            }
        }
    }
    return body

def make_rec_query(state='NY', keywords=[]):
    if r.status_code == 200:
        results = es.search(index='uniq',body=recs_query(state, keywords))
        return results['hits']['hits'][1:]
    return

# Read in tags and state options
def read_pickle(bucket, key):
    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    pickled = pickle.loads(s3.Bucket(bucket).Object(key).get()['Body'].read())
    return pickled

top_tags = read_pickle('jd-parquet', 'assets/tag_count.pkl')
state_list = read_pickle('jd-parquet', 'assets/state.pkl')

# top_tags = [('Java', 1000), ('Python', 1000), ('Go', 1000),
#     ('Python1', 1000), ('Python2', 1000), ('Python3', 1000),
#     ('Python4', 1000), ('Python5', 1000), ('Python6', 1000),
#     ('Python7', 1000)]
# state_list = ['TX', 'MA', 'CA']

# Header navigation bar
app.layout = html.Div([
    html.Div(dcc.Location(id='url', refresh=False)),
    html.Div([
        html.H2('JobMiner', className = 'w3-bar-item w3-button w3-text-dark-grey w3-center'),
        html.Div([
            html.A(id='gh-link',
                children=['View on GitHub'],
                href="https://github.com/billhuang56/job-sniper",
                className = 'w3-bar-item w3-button'),
            html.I(id='gh-logo',
                className = 'fa fa-github fa-fw w3-margin-right w3-xxlarge')
         ], className = 'w3-right w3-hide-small w3-bar-item')
    ], className = 'w3-bar w3-white w3-wide w3-padding w3-card'),
    html.Div([
        html.Div(
            html.Div([
                html.Div([
                    dcc.Dropdown(id='states',
                        options=[{'label': s, 'value': s} for s in state_list],
                        placeholder = 'Select your state',
                        value="NY",
                        className='w3-input w3-margin-bottom'
                    ),
                    dcc.Dropdown(id='tags',
                        options=[{'label': t[0].capitalize(), 'value': t[0].capitalize()} for t in top_tags],
                        multi=True,
                        placeholder = 'Put in 5 tags or more...',
                        value='',
                        className='w3-input w3-padding'
                    ),
                    html.Button('Search',
                        id='search-btn',
                        n_clicks_timestamp=0,
                        className = 'w3-button w3-block w3-teal w3-section'
                    )
                ]),
                html.Div([
                    dbc.Button('Python',
                        id = 'tag-1', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Java',
                        id = 'tag-2', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Spark',
                        id = 'tag-3', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Database',
                        id = 'tag-4', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Aws',
                        id = 'tag-5', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Elasticsearch',
                        id = 'tag-6', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Postgresql',
                        id = 'tag-7', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Architecture',
                        id = 'tag-8', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('Sql',
                        id = 'tag-9', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    ),
                    dbc.Button('GitHub',
                        id = 'tag-10', n_clicks_timestamp=0,
                        className='w3-auto w3-blue tags4'
                    )
                ])
            ], className='w3-dark-grey w3-margin-right w3-padding-large w3-round-large'),
            className='w3-quarter'
        ),
        html.Div([
            dbc.Fade([
                html.Div([
                    html.Div([
                        html.Button('Next Job',id='next-btn', className='w3-right w3-show-inline-block w3-button w3-teal'),
                        html.Div(id='page', className='w3-center w3-show-inline-block')
                    ], className= 'w3-bar w3-white w3-margin-top'),
                    html.Div([
                        html.I(id='suitcase',
                            className='fa fa-suitcase fa-fw w3-margin-right w3-xxlarge w3-text-teal w3-show-inline-block',
                            ),
                        html.H2(id='job-title', children = 'hi',
                            className='w3-text-grey w3-padding-16 w3-show-inline-block'),
                    ]),
                    html.Div([
                        html.I(id='calendar',
                            className='fa fa-calendar fa-fw w3-margin-right w3-xlarge w3-text-teal w3-show-inline-block'),
                        html.H3(id='date', children = 'tome',
                            className='w3-text-teal w3-show-inline-block'),
                    ]),
                    html.P(id='job-description', children='hi'),
                    html.Div([
                        html.Button('',
                            id='jd-tag-1',
                            className = 'w3-auto w3-blue tags4 w3-margin-bottom'
                        ),
                        html.Button('',
                            id='jd-tag-2',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-3',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-4',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-5',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-6',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-7',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-8',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-9',
                            className = 'w3-auto w3-blue tags4'
                        ),
                        html.Button('',
                            id='jd-tag-10',
                            className = 'w3-auto w3-blue tags4'
                        )
                    ])
                ], className= 'w3-container w3-card w3-white w3-margin-bottom'),
                html.Div([
                    html.Div([
                        html.Div([
                        html.H5(id= 'rec-title-1', children = '', className="card-title"),
                        html.P(id = 'rec-company-1', children = ''),
                        html.Button("Details", id= 'rec-btn-1', n_clicks_timestamp=0,
                            className="w3-right w3-button w3-teal")
                        ], className='w3-container w3-card w3-white w3-padding-16')
                    ], className='w3-quarter'
                    ),
                    html.Div([
                        html.Div([
                            html.H5(id= 'rec-title-2', children = '', className="card-title"),
                            html.P(id = 'rec-company-2', children = ''),
                            html.Button("Details", id= 'rec-btn-2', n_clicks_timestamp=0,
                                className="w3-right w3-button w3-teal")
                        ], className='w3-container w3-card w3-white w3-padding-16')
                    ], className='w3-quarter'
                    ),
                    html.Div([
                        html.Div([
                            html.H5(id= 'rec-title-3', children = '', className="card-title"),
                            html.P(id = 'rec-company-3', children = ''),
                            html.Button("Details", id= 'rec-btn-3', n_clicks_timestamp=0,
                                className="w3-right w3-button w3-teal")
                        ], className='w3-container w3-card w3-white w3-padding-16')
                    ], className='w3-quarter'
                    ),
                    html.Div([
                        html.Div([
                            html.H5(id= 'rec-title-4', children = '', className="card-title"),
                            html.P(id = 'rec-company-4', children = ''),
                            html.Button("Details", id='rec-btn-4', n_clicks_timestamp=0,
                            className="w3-right w3-button w3-teal")
                        ], className='w3-container w3-card w3-white w3-padding-16')
                    ], className='w3-quarter'
                    ),
                ], className='w3-row-padding w3-padding-16'),
                html.Div(id='new-query', style={'display': 'none'}),
                html.Div(id='rec-query', style={'display': 'none'}),
                html.Div(id='new-page', style={'display': 'none'})
            ],
            id="fade",
            is_in=False,
            appear=True,
            )
        ], className='w3-threequarter')
    ], className='w3-content w3-margin-top')
])

# User click-enter tags
@app.callback(
    Output('tags', 'value'),
    [Input('tag-1', 'children'),
    Input('tag-1', 'n_clicks_timestamp'),
    Input('tag-2', 'children'),
    Input('tag-2', 'n_clicks_timestamp'),
    Input('tag-3', 'children'),
    Input('tag-3', 'n_clicks_timestamp'),
    Input('tag-4', 'children'),
    Input('tag-4', 'n_clicks_timestamp'),
    Input('tag-5', 'children'),
    Input('tag-5', 'n_clicks_timestamp'),
    Input('tag-6', 'children'),
    Input('tag-6', 'n_clicks_timestamp'),
    Input('tag-7', 'children'),
    Input('tag-7', 'n_clicks_timestamp'),
    Input('tag-8', 'children'),
    Input('tag-8', 'n_clicks_timestamp'),
    Input('tag-9', 'children'),
    Input('tag-9', 'n_clicks_timestamp'),
    Input('tag-10', 'children'),
    Input('tag-10', 'n_clicks_timestamp')],
    [State('tags', 'value')])
def search_tag_click(tag1, nc1, tag2, nc2, tag3, nc3, tag4, nc4, tag5, nc5,
    tag6, nc6, tag7, nc7, tag8, nc8, tag9, nc9, tag10, nc10, existed_tags):
    if not existed_tags:
        existed_tags = set()
    else:
        existed_tags = set(existed_tags,)
    btn_list = [int(nc1), int(nc2), int(nc3), int(nc4), int(nc5), int(nc6),
        int(nc7), int(nc8), int(nc9), int(nc10)]
    index = btn_list.index(max(btn_list))
    if index == 0:
        existed_tags.update([tag1])
    if index == 1:
        existed_tags.update([tag2])
    if index == 2:
        existed_tags.update([tag3])
    if index == 3:
        existed_tags.update([tag4])
    if index == 4:
        existed_tags.update([tag5])
    if index == 5:
        existed_tags.update([tag6])
    if index == 6:
        existed_tags.update([tag7])
    if index == 7:
        existed_tags.update([tag8])
    if index == 8:
        existed_tags.update([tag9])
    if index == 9:
        existed_tags.update([tag10])
    return list(existed_tags)

# Search results set to be hidden before the a search is made
@app.callback(
    Output('fade', 'is_in'),
    [Input('search-btn', 'n_clicks')])
def show_results(n_clicks):
    if n_clicks:
        return True

# Initiate a new search and pass on the results to an intermediate holder
@app.callback(
    Output('new-query', 'children'),
    [Input('search-btn', 'n_clicks_timestamp'),
    Input('rec-btn-1', 'n_clicks_timestamp'),
    Input('rec-btn-2', 'n_clicks_timestamp'),
    Input('rec-btn-3', 'n_clicks_timestamp'),
    Input('rec-btn-4', 'n_clicks_timestamp')],
    [State('states', 'value'),
    State('tags', 'value'),
    State('rec-query', 'children')])
def recieve_query(search_click, nc1, nc2, nc3, nc4, state, tags, rec_query):
    query=''
    if rec_query:
        rec_query = json.loads(rec_query)
    index = -1
    btn_list = [int(search_click), int(nc1), int(nc2), int(nc3), int(nc4)]
    index = btn_list.index(max(btn_list))
    if index == 0:
        query = make_new_query(state, tags)
    if index == 1 and len(rec_query) > 0:
        query = [rec_query[0]]
    if index == 2 and len(rec_query) > 1:
        query = [rec_query[1]]
    if index == 3 and len(rec_query) > 2:
        query = [rec_query[2]]
    if index == 4 and len(rec_query) > 3:
        query = [rec_query[3]]
    return json.dumps(query)

# Reset the next button
@app.callback(
    Output('next-btn', 'n_clicks'),
    [Input('new-query', 'children')])
def reset_next(query):
    return None

# Prepare page information
@app.callback(
    [Output('new-page', 'children'),
    Output('rec-query', 'children')],
    [Input('new-query', 'children'),
    Input('next-btn', 'n_clicks')])
def recieve_query(query, next_click):
    query = json.loads(query)
    if not query:
        return '', ''
    if not next_click:
        page_query = query[0]['_source']
    else:
        page_query = query[int(next_click) % len(query)]['_source']
    rec_query = make_rec_query(page_query['state'], page_query['keywords'])
    return json.dumps(page_query), json.dumps(rec_query)

# Populate the page with search results
@app.callback(
    [Output('job-title', 'children'),
     Output('date', 'children'),
     Output('page', 'children'),
     Output('job-description', 'children'),
     Output('jd-tag-1', 'children'),
     Output('jd-tag-2', 'children'),
     Output('jd-tag-3', 'children'),
     Output('jd-tag-4', 'children'),
     Output('jd-tag-5', 'children'),
     Output('jd-tag-6', 'children'),
     Output('jd-tag-7', 'children'),
     Output('jd-tag-8', 'children'),
     Output('jd-tag-9', 'children'),
     Output('jd-tag-10', 'children'),
     Output('rec-title-1', 'children'),
     Output('rec-title-2', 'children'),
     Output('rec-title-3', 'children'),
     Output('rec-title-4', 'children'),
     Output('rec-company-1', 'children'),
     Output('rec-company-2', 'children'),
     Output('rec-company-3', 'children'),
     Output('rec-company-4', 'children')],
    [Input('new-page', 'children'),
    Input('rec-query', 'children')],
    [State('new-query', 'children')])
def search_result(content, rec_query, query):
    if not content:
        return ['No results'] + [''] * 21
    content = json.loads(content)
    query = json.loads(query)
    output_list = []
    output_list.append(content['job_title'] + ' @ ' + content['company_name'] + ', ' + content['state'])
    output_list.append(content['post_date'])
    output_list.append('{} postings'.format(len(query)))
    output_list.append(content['job_description'])
    if len(content['tags']) < 10:
        tags_list = content['tags'] + [''] * (10 - len(content['tags']))
    else:
        tags_list = content['tags'][:10]
    output_list.extend(tags_list)
    if not rec_query:
        rec_title  = [''] * 4
        rec_comp  = [''] * 4
    else:
        rec_query = json.loads(rec_query)
        rec_title = [rec['_source']['job_title'] for rec in rec_query]
        rec_comp = [rec['_source']['company_name'] for rec in rec_query]
    if len(rec_title) < 4:
        rec_title = rec_title + [''] * (4 - len(rec_title))
        rec_comp = rec_comp + [''] * (4 - len(rec_comp))
    output_list.extend(rec_title)
    output_list.extend(rec_comp)
    return output_list

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=80)
