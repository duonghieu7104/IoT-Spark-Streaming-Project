from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pymongo import MongoClient
import os
from datetime import datetime

# ========================
# Cấu hình MongoDB
# ========================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = "iotdb"
COLLECTION_NAME = "robot_alerts"

def save_to_mongo(records, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    """
    Lưu danh sách records vào MongoDB
    """
    if records:
        client = MongoClient(MONGO_URI)
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(records)
        client.close()
        print(f"✅ Saved {len(records)} records to {db_name}.{collection_name}")
    else:
        print("⚠️ No records to save in this batch.")

# ========================
# Xử lý mỗi RDD
# ========================
def process_rdd(rdd):
    """
    RDD mỗi batch: [(robot_id, (pin_low_count, stuck_count))]
    """
    records = rdd.collect()
    alert_records = []

    for robot_id, (pin_low_count, stuck_count) in records:
        alert = {}
        if pin_low_count > 0:
            alert['low_battery_count'] = pin_low_count
        if stuck_count > 0:
            alert['stuck_count'] = stuck_count
        if alert:
            alert_record = {
                "robot_id": robot_id,
                "alert": alert,
                "timestamp": datetime.utcnow()
            }
            alert_records.append(alert_record)

    save_to_mongo(alert_records)

# ========================
# Main: Spark Streaming
# ========================
if __name__ == "__main__":
    sc = SparkContext("local[2]", "RobotIoTStream")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    ssc.checkpoint("/tmp/spark_checkpoint_robot")

    lines = ssc.socketTextStream("localhost", 9998)

    parsed = lines.map(lambda line: line.strip().split(",")) \
                  .filter(lambda parts: len(parts) == 6) \
                  .map(lambda parts: (
                      parts[0],
                      (
                          1 if int(parts[4]) < 20 else 0,        
                          1 if parts[5].lower() == "true" else 0 
                      )
                  ))

    windowed_counts = parsed.reduceByKeyAndWindow(
        lambda a, b: (a[0]+b[0], a[1]+b[1]),
        lambda a, b: (a[0]-b[0], a[1]-b[1]),
        600,
        1
    )

    windowed_counts.foreachRDD(process_rdd)

    print("📡 Spark Streaming started, listening on localhost:9998")
    ssc.start()
    ssc.awaitTermination()
