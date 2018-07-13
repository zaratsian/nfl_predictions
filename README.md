<h3>Spark NFL Predictions</h3>
This project deploys a pyspark model that is used to predict the next NLF play. Specfically, there are two models and I trained on historical NFL Play-by-Play data. There's one model that predicts the expected yards gained or lost for a running play, and similarly a model that predicts yards gained or lost for a passing play. The model will output the expected yardage as well as which play (running or passing) should be executed. 
<br>
<br><b>To run:</b>
<br>
<br>NOTE: Before you begin, make sure you have Docker installed. This app is deployed and runs within a docker container.
<br>
<br>&nbsp;&nbsp;&nbsp;&nbsp;1) Download this repo
<br>&nbsp;&nbsp;&nbsp;&nbsp;2) <code>./build.sh</code>
<br>&nbsp;&nbsp;&nbsp;&nbsp;3) <code>./run.sh</code>
<br>&nbsp;&nbsp;&nbsp;&nbsp;4) Wait a minute or two, then open up your browser and go to <a href="http://localhost:14444">http://localhost:14444</a>
<br>
<br><b>Bonus (for real-time scoring):</b>
<br>I have included an <a href="https://nifi.apache.org/">Apache NiFi</a> template, called <a href="https://github.com/zaratsian/nfl_predictions/blob/master/NFL_Spark_Model_Scoring.xml">NFL_Spark_Model_Scoring.xml</a>. This template can be loaded into Apache NiFi. Once loaded, it'll enable users to ingest, process, and score real-time play-by-play data against this REST-based Spark Web App. The incoming data stream may come from an API, a message bus (such as Kafka or JMS), or another real-time (or static) data source.
<br>
<br><img src="images/screenshot1.png" class="inline"/>
<br>
<br>
