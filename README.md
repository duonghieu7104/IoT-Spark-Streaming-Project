bt1
docker exec -it spark-master spark-submit /app/bt1/input_stream.py
docker exec -it spark-master spark-submit /app/bt1/spark_foreachRDD.py

bt2
docker exec -it spark-master spark-submit /app/bt2/input_stream.py
docker exec -it spark-master spark-submit /app/bt2/main.py