

import os,sys,re, csv
import random
import json
from flask import Flask, render_template, json, request, redirect, jsonify, url_for, session
from werkzeug.utils import secure_filename
#import flask_login
import requests
import datetime, time
import numpy as np


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


header, rawdata = import_csv( os.path.join( os.getcwd(), 'static/assets/nfldata2.csv') )
header = data_header


def get_next_play(rawdata, row_number):
    record = rawdata[row_number]
    date   = record[0][0:10]
    return record, date


def predict_play(model_pass, model_run, qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, posteam, DefensiveTeam, PlayType_lag):
    
    input_df = spark.createDataFrame( [(qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, posteam, DefensiveTeam, PlayType_lag)], ['qtr','down','TimeSecs','yrdline100','ydstogo','ydsnet','month_day', 'posteam', 'DefensiveTeam', 'PlayType_lag'])
    
    passing_yards = model_pass.transform(input_df).select('prediction').collect()[0][0]
    running_yards = model_run.transform(input_df).select('prediction').collect()[0][0]
    
    best_play     = 'Passing Play' if passing_yards > running_yards else 'Running Play'
    
    return best_play, passing_yards, running_yards


def predict_play_livy(model_pass, model_run, qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, PlayType_lag):
    
    code_payload = {'code': '''
    
    import datetime, time
    import re, random, sys
    from pyspark.ml import PipelineModel
    
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    
    spark = SparkSession \
        .builder \
        .appName("pyspark_nfl_prediction") \
        .getOrCreate()
    
    #Drive          = ''' + str(1) + '''
    qtr             = ''' + str(qtr) + '''
    down            = ''' + str(down) + '''
    #TimeUnder      = ''' + str(15) + '''
    TimeSecs        = ''' + str(TimeSecs) + '''
    #PlayTimeDiff   = ''' + str(20) + '''
    yrdline100      = ''' + str(yrdline100) + '''
    ydstogo         = ''' + str(ydstogo) + '''
    ydsnet          = ''' + str(ydsnet) + '''
    month_day       = ''' + str(month_day) + '''
    PlayType_lag    = "''' + str(PlayType_lag) + '''"
    
    input_df = spark.createDataFrame( [(qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, PlayType_lag)], ['qtr','down','TimeSecs','yrdline100','ydstogo','ydsnet','month_day','PlayType_lag'])
    
    model_pass = PipelineModel.load('/models/nfl_model_pass')
    model_run  = PipelineModel.load('/models/nfl_model_run')
    
    passing_yards = model_pass.transform(input_df).select('prediction').collect()[0][0]
    running_yards = model_run.transform(input_df).select('prediction').collect()[0][0]
    
    best_play     = 'Passing Play' if passing_yards > running_yards else 'Running Play'
    
    print({'passing_yards':passing_yards, 'running_yards':running_yards, 'best_play':best_play})
    
    '''}
    
    if session_id:
        session_id, result = spark_livy_interactive(host='dzaratsian4.field.hortonworks.com', port='8999', code_payload=code_payload, session_id=session_id)
    else:
        session_id, result = spark_livy_interactive(host='dzaratsian4.field.hortonworks.com', port='8999', code_payload=code_payload, session_id='')


def start_livy_and_load_model(host='dzaratsian4.field.hortonworks.com', port='8999'):
    
    code_payload = {'code': '''
    
    import datetime, time
    import re, random, sys
    from pyspark.ml import PipelineModel
    
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    
    spark = SparkSession \
        .builder \
        .appName("pyspark_nfl_prediction") \
        .getOrCreate()
    
    model_pass = PipelineModel.load('./static/assets/nfl_model_pass')
    model_run  = PipelineModel.load('./static/assets/nfl_model_run')
    
    '''}
    
    session_id, result = spark_livy_interactive(host=host, port=port, code_payload=code_payload, session_id='')
    return session_id, result



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
        posteam         = request.form.get('posteam','')
        DefensiveTeam   = request.form.get('DefensiveTeam','')
        #drive          = request.form.get('drive','')
        qtr             = int(request.form.get('quarter',''))
        down            = int(request.form.get('down',''))
        TimeSecs        = int(request.form.get('timesecs',''))
        yrdline100      = int(request.form.get('yrdline100',''))
        ydstogo         = int(request.form.get('ydstogo',''))
        ydsnet          = int(request.form.get('ydsnet',''))
        month_day       = int( datestamp[5:7] + datestamp[8:10] )
        PlayType_lag    = request.form.get('playtype_lag','')
        
        best_play, passing_yards, running_yards = predict_play(model_pass, model_run, qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, posteam, DefensiveTeam, PlayType_lag)
        #best_play, passing_yards, running_yards = predict_play(model_pass, model_run, qtr, down, TimeSecs, yrdline100, ydstogo, ydsnet, month_day, posteam, DefensiveTeam, PlayType_lag)
        
        row_number = row_number + 1
        next_play, date = get_next_play(rawdata, row_number)
        
        #return render_template('index.html', playtypes=playtypes, next_play=next_play, date=date, row_number=row_number)
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
