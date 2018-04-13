
'''
/spark/bin/spark-submit app_nfl.py 
'''

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

################################################################################################
#
#   Functions
#
################################################################################################

def import_csv(filepath_with_name):
    try:
        file = csv.reader(open(filepath_with_name, 'rb'))
        
        # Header
        header  = file.next()
        
        row_count = 0
        rows      = []
        for row in file:
            rows.append(row)
            row_count = row_count + 1
        
        col_count = len(row)
    except: 
        rows = 'Error in data location or format'
        header    = ''
        col_count = ''
        row_count = ''
    
    return header, rows


header, rawdata = import_csv('/assets/static/assets/nfldata2.csv')


def get_next_play(rawdata, row_number):
    record = rawdata[row_number]
    date   = record[0][0:10]
    return record, date


def start_connect_to_livy_session(host='', port='8999'):
    base_url    = 'http://' + str(host) +':'+ str(port)
    session_url = base_url + '/sessions'
    headers     = {'Content-Type': 'application/json', 'X-Requested-By': 'spark'}
    try:
        response   = requests.get(session_url).json()
        session_id = response['sessions'][0]['id']
    except:
        session_id = ''
    return session_id


def spark_livy_interactive(host='', port='8999', code_payload='', session_id=''):
    
    result      = ''
    base_url    = 'http://' + str(host) +':'+ str(port)
    session_url = base_url + '/sessions'
    headers     = {'Content-Type': 'application/json', 'X-Requested-By': 'spark'}
    
    if session_id == '':
        #data = {'kind': 'pyspark'}
        data = {'kind': 'pyspark', 'heartbeatTimeoutInSecond':600, 'conf':{'spark.yarn.appMasterEnv.PYSPARK_PYTHON':'/opt/anaconda2/bin/python2.7'}}
        spark_session = requests.post(base_url + '/sessions', data=json.dumps(data), headers=headers)
        if spark_session.status_code == 201:
            session_id  = spark_session.json()['id']
            session_url = base_url + spark_session.headers['location']
            print('[ INFO ] Status Code:           ' + str(spark_session.status_code))
            print('[ INFO ] Session State:         ' + str(spark_session.json()['state']))
            print('[ INFO ] Session ID:            ' + str(session_id))
            print('[ INFO ] Payload:               ' + str(spark_session.content))
            
            # Loop until Spark Session is ready (i.e. In the Idle State)
            session_state = ''
            while (session_state == '') or (session_state == 'starting'):
                time.sleep(0.25)
                print('[ INFO ] Session State:         ' + str(session_state))
                session = requests.get(session_url, headers=headers)
                if session.status_code == 200:
                    session_state = session.json()['state']
                else:
                    print('[ ERROR ] Status Code: ' + str(session.status_code))
                    session_state = 'end'
            
            print('[ INFO ] Session State:         ' + str(session_state))
            print('[ INFO ] Spark App ID:          ' + str(session.json()['appId']))
            print('[ INFO ] Spark App URL:         ' + str(session.json()['appInfo']['sparkUiUrl']))
        else:
            print('[ ERROR ] Failed to start Spark Session, Status Code: ' + str(spark_session.status_code))
    else:
        print('[ INFO ] Using existing Session ID: ' + str(session_id))
    
    try:
        submit_code = requests.post(base_url + '/sessions/' + str(session_id) + '/statements', data=json.dumps(code_payload), headers=headers)
    except:
        result = {'state':'Error with code submission'}
        return session_id, result
    
    if submit_code.status_code == 201:
        submit_code_state = ''
        while (submit_code_state == '') or (submit_code_state == 'running'):
            time.sleep(0.25)
            code_response = requests.get(base_url + submit_code.headers['location'], headers=headers)
            if code_response.status_code == 200:
                result = code_response.json()
                submit_code_state = code_response.json()['state']
                print('[ INFO ] Code Submit State:     ' + str(submit_code_state))
                #print('\n' + '#'*50)
                #print('[ INFO ] Result:   ' + str(result))
                #print('#'*50)
            else:
                print('[ ERROR ] Status Code:   ' + str(code_response.status_code))
    else:
        print('[ ERROR ] Failed to submit Spark code successfully, Status Code: ' + str(submit_code.status_code))
     
    return session_id, result


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
    
    from pyspark.sql import SparkSession
    from pyspark.ml import PipelineModel
    
    spark = SparkSession \
        .builder \
        .config("spark.driver.allowMultipleContexts", "true") \
        .appName("pyspark_nfl_app") \
        .getOrCreate()
    
    model_pass = PipelineModel.load('/assets/static/assets/nfl_model_pass')
    model_run  = PipelineModel.load('/assets/static/assets/nfl_model_run')    
    #model_pass = PipelineModel.load('./static/assets/nfl_model_pass')
    #model_run  = PipelineModel.load('./static/assets/nfl_model_run')
    
    #app.run(debug=True, threaded=False, host='0.0.0.0', port=4444)
    app.run(threaded=False, host='0.0.0.0', port=4444)



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
