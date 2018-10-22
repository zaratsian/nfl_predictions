

import os,sys,re, csv
import random
import json
from flask import Flask, render_template, json, request, redirect, jsonify, url_for, session
#from werkzeug.utils import secure_filename
#import flask_login
import requests
import datetime, time
import numpy as np
import argparse
import pandas as pd

from sklearn.externals import joblib

################################################################################################
#
#   Flask App
#
################################################################################################

app = Flask(__name__)
app.secret_key = os.urandom(24)

################################################################################################
#
#   Global Variables
#
################################################################################################

playtypes       = ['FirstPlay','Run','Pass']

list_of_teams   = [u'NYJ', u'CAR', u'TB', u'OAK', u'DET', u'TEN', u'BUF', u'BAL', u'NE', u'GB', u'JAC', u'DEN', u'ARI', u'SF', u'KC', u'SEA', u'CIN', u'DAL', u'CLE', u'MIA', u'SD', u'STL', u'MIN', u'ATL', u'PHI', u'WAS', u'NYG', u'PIT', u'NO', u'IND', u'HOU', u'CHI']

data_header     = ['Date', 'GameID', 'Drive', 'qtr', 'down', 'time', 'TimeUnder', 'TimeSecs', 'PlayTimeDiff', 'yrdline100', 'ydstogo', 'ydsnet', 'FirstDown', 'posteam', 'DefensiveTeam', 'Yards_Gained', 'Touchdown', 'PlayType', 'PassLength', 'PassLocation', 'RunLocation', 'PosTeamScore', 'DefTeamScore', 'month_day', 'PlayType_lag']


################################################################################################
#
#   Functions
#
################################################################################################

def import_csv(filepath_with_name, header=False):
    header = ''
    rows   = []
    try:
        csv_file = open(filepath_with_name)
        csv_reader = csv.reader(csv_file, delimiter=',')
        for i,row in enumerate(csv_reader):
            try:
                if (i == 0) and (header):
                    header = row
                else:
                    rows.append(row)
            except:
                pass
        
        csv_file.close()
    except:
        rows = 'Error in data location or format'
        header    = ''
    
    return header, rows


#header, rawdata = import_csv( os.path.join( os.getcwd(), 'static/assets/nfldata2.csv') )
#header = data_header


def load_models():
    nfl_model_running = os.path.join( os.getcwd(), 'static/assets/nfl_model_running.joblib')
    nfl_model_passing = os.path.join( os.getcwd(), 'static/assets/nfl_model_passing.joblib')
    return nfl_model_running, nfl_model_passing

#nfl_model_running, nfl_model_passing = load_models()


def get_next_play(rawdata, row_number):
    record = rawdata[row_number]
    date   = record[0][0:10]
    return record, date


def score_data(path_to_model, df_to_score):
    
    #print('[ INFO ] Loading model from {}'.format(path_to_model))
    model_obj = joblib.load(path_to_model)
    
    scores = model_obj.predict(df_to_score)
    df_to_score['predicted'] = scores
    
    # Returns dataframe with predictions appended to last column
    return df_to_score


def transform_df(rawdata, target_variable_name=None):
    
    # Model Variables (Specify id, target, numeric variables, and categorical variables)
    var_id              = ''
    var_target          = target_variable_name #'Yards_Gained'
    var_date            = 'Date'
    var_numeric         = ['Drive', 'qtr', 'down', 'TimeSecs', 'PlayTimeDiff', 'yrdline100', 'ydstogo', 'ydsnet', 'FirstDown', 'PosTeamScore', 'DefTeamScore'] # 'month_day']
    var_category        = ['posteam', 'DefensiveTeam','PlayType_lag'] # 'PlayType']
    
    transformed_set             = {}
    if var_target != None:
        transformed_set[var_target] = rawdata[var_target]
    
    for var in var_numeric:
        transformed_set[var] = rawdata[var]
    
    '''
    for var in var_category:
        transformed_set[var] = rawdata[var].astype('category').cat.codes
    '''
    
    category_coding = {}
    for var in var_category:
        category_coding[var] = dict( enumerate( rawdata[var].astype('category').cat.categories ))
        transformed_set[var] = rawdata[var].astype('category').cat.codes
    
    extracted_year  = pd.to_datetime(rawdata[var_date]).dt.year
    extracted_month = pd.to_datetime(rawdata[var_date]).dt.month
    extracted_day   = pd.to_datetime(rawdata[var_date]).dt.day
    
    transformed_set['year']  = extracted_year
    transformed_set['month'] = extracted_month
    transformed_set['day']   = extracted_day
    
    # Create transformed DF
    transformed_df = pd.concat([v for k,v in transformed_set.items()], axis=1)
    transformed_df.columns = [k for k,v in transformed_set.items()]
    transformed_df.head()
    return transformed_df


def predict_run_play(nfl_model_path, variable_dict, PlayType):
    # PlayType is either "Run" or "Pass" 
    
    row_number   = int(request.form.get('row_number',''))
    #variable_dict['PlayType'] = PlayType
    rawdata = pd.DataFrame(variable_dict, index=[0])
    
    # Transform / Prep dataframe
    transformed_df = transform_df(rawdata, None)
    
    # Score Data
    scored_df = score_data(nfl_model_path, transformed_df)
    #scored_df['actual'] = rawdata['Yards_Gained']
    
    prediction = round(float(scored_df['predicted']),2)
    
    return prediction


###############################################################################################

# Load Rawdata
header, rawdata = import_csv( os.path.join( os.getcwd(), 'static/assets/nfldata2.csv') )
header = data_header


# Load models
nfl_model_running, nfl_model_passing = load_models()


################################################################################################
#
#   Index
#
################################################################################################
@app.route('/', methods = ['GET','POST'])
@app.route('/index', methods = ['GET','POST'])
def index():
    
    if request.method == 'GET':
        row_number = 0
        next_play, date = get_next_play(rawdata, row_number)
        
        return render_template('index.html', playtypes=playtypes, posteams=list_of_teams, DefensiveTeams=list_of_teams, next_play=next_play, date=date, row_number=row_number)
    
    if request.method == 'POST':
        row_number      = int(request.form.get('row_number',''))
        datestamp       = request.form.get('datestamp','')
        variable_dict = {
            'Date':         str(datestamp) + 'T00:00:00.000Z',
            'Drive':        int(random.randint(1,6)),
            'qtr':          int(request.form.get('quarter','')),
            'down':         int(request.form.get('down','')),
            'TimeSecs':     int(request.form.get('timesecs','')),
            'PlayTimeDiff': int(random.randint(1,35)),
            'yrdline100':   int(request.form.get('yrdline100','')),
            'ydstogo':      int(request.form.get('ydstogo','')),
            'ydsnet':       int(request.form.get('ydsnet','')),
            'FirstDown':    int(random.randint(0,1)),
            'PosTeamScore': int(random.randint(1,40)),
            'DefTeamScore': int(random.randint(1,30)),
            'month_day':    int( datestamp[5:7] + datestamp[8:10] ),
            'posteam':      request.form.get('posteam',''),
            'DefensiveTeam':request.form.get('DefensiveTeam',''),
            'PlayType_lag': request.form.get('playtype_lag',''),
        }
        
        running_yards = predict_run_play(nfl_model_running, variable_dict, "Run")
        passing_yards = predict_run_play(nfl_model_passing, variable_dict, "Pass")
        best_play     = 'Passing Play' if passing_yards > running_yards else 'Running Play'
        
        row_number = row_number + 1
        next_play, date = get_next_play(rawdata, row_number)
        
        return render_template('index.html', playtypes=playtypes, posteams=list_of_teams, DefensiveTeams=list_of_teams, next_play=next_play, date=date, row_number=row_number, best_play=best_play, passing_yards=round(passing_yards,2), running_yards=round(running_yards,2))


################################################################################################
#
#   API
#
################################################################################################
@app.route('/api', methods = ['GET','POST'])
def api():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"qtr":3, "down":3, "TimeSecs":60, "yrdline100":50, "ydstogo":8, "ydsnet":15, "month_day":920, "posteam":"PIT", "DefensiveTeam":"NE", "PlayType_lag":"Run" }' http://dzaratsian6.field.hortonworks.com:4444/api/
        '''
        qtr             = request.json['qtr']
        down            = request.json['down']
        TimeSecs        = request.json['TimeSecs']
        yrdline100      = request.json['yrdline100']
        ydstogo         = request.json['ydstogo']
        ydsnet          = request.json['ydsnet']
        month_day       = request.json['month_day']
        posteam         = request.json['posteam']
        DefensiveTeam   = request.json['DefensiveTeam']
        PlayType_lag    = request.json['PlayType_lag']
        
        ##Drive          = 1
        #posteam         = 'PIT'
        #DefensiveTeam   = 'NE'
        #qtr             = 3
        #down            = 3
        ##TimeUnder      = 14
        #TimeSecs        = 2000
        ##PlayTimeDiff   = 30
        #yrdline100      = 50
        #ydstogo         = 8
        #ydsnet          = 15
        #month_day       = 920
        #PlayType_lag    = 'Run'
        
        best_play, passing_yards, running_yards = predict_play(model_pass, model_run, qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, posteam, DefensiveTeam, PlayType_lag)
        #('Running Play', 3.984419701538829, 5.375515688399204)
        
        return jsonify( {"best_play":best_play, "passing_yards":passing_yards, "running_yards":running_yards } )



################################################################################################
#
#   Run App
#
################################################################################################

if __name__ == "__main__":
    
    #nfl_model_running = os.path.join( os.getcwd(), 'static/assets/nfl_model_running.joblib')
    #nfl_model_passing = os.path.join( os.getcwd(), 'static/assets/nfl_model_passing.joblib')
    
    #app.run(debug=True, threaded=False, host='0.0.0.0', port=4444)
    app.run(threaded=False, host='0.0.0.0', port=8500)



'''

0   Date
1   GameID
2   Drive
3   qtr
4   down
5   time
6   TimeUnder
7   TimeSecs
8   PlayTimeDiff
9   yrdline100
10  ydstogo
11  ydsnet
12  FirstDown
13  posteam
14  DefensiveTeam
15  Yards_Gained
16  Touchdown
17  PlayType
18  PassLength
19  PassLocation
20  RunLocation
21  PosTeamScore
22  DefTeamScore
23  month_day
24  PlayType_lag

'''


#ZEND
