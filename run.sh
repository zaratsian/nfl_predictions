docker stop nfl_predictions
docker rm -f nfl_predictions

docker run -it -d -p 18080:8080 -p 14444:4444 --hostname nfl_predictions --net dev --name nfl_predictions nfl_predictions

# Copy Assets
docker exec nfl_predictions /spark/bin/spark-submit /assets/app_nfl.py &

echo "*****************************************************"
echo "*"
echo "*  Container has been started..."
echo "*"
echo "*  App is running at http://localhost:14444"
echo "*"
echo "*  Usage: docker exec -it nfl_predictions bash"
echo "*"
echo "*****************************************************"
echo ""
echo ""
echo ""








