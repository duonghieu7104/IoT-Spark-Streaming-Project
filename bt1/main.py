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
COLLECTION_NAME = "gas_sensor"

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
    records = []
    alert_records = []

    lines = rdd.collect()
    if not lines:
        print("⚠️ Received empty RDD batch")
        return

    for line in lines:
        try:
            parts = line.strip().split(",")

            ### LOGIC XỬ LÝ DỮ LIỆU VÀ PHÁT HIỆN CẢNH BÁO ###
            # 1. Kiểm tra số cột đúng
            if len(parts) != 5:
                print(f"⚠️ Skipping line, unexpected number of columns: {line}")
                continue

            sensor_name = parts[0]
            value1 = float(parts[1])
            value2 = float(parts[2])
            value3 = float(parts[3])
            status = parts[4]

            # 2. Tạo record bình thường
            record = {
                "sensor": sensor_name,
                "value1": value1,
                "value2": value2,
                "value3": value3,
                "status": status,
                "timestamp": datetime.utcnow()
            }
            records.append(record)

            # 3. Kiểm tra điều kiện cảnh báo
            alert = None
            if value1 > 500:
                alert = "High gas level"
            elif value3 > 60:
                alert = "High temperature"

            # 4. Nếu có cảnh báo, thêm vào alert_records
            if alert:
                alert_record = {
                    "sensor": sensor_name,
                    "alert": alert,
                    "values": {"v1": value1, "v2": value2, "v3": value3},
                    "timestamp": datetime.utcnow()
                }
                alert_records.append(alert_record)
            ### END LOGIC ###

        except Exception as e:
            print(f"⚠️ Skipping line due to error: {line} | {e}")

    save_to_mongo(records, DB_NAME, COLLECTION_NAME)
    if alert_records:
        save_to_mongo(alert_records, DB_NAME, COLLECTION_NAME + "_alerts")

# ========================
# Main: Spark Streaming
# ========================
if __name__ == "__main__":
    sc = SparkContext("local[2]", "IoTStreamApp")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream("localhost", 9998)
    lines.foreachRDD(process_rdd)

    print("📡 Spark Streaming started, listening on localhost:9998")
    ssc.start()
    ssc.awaitTermination()
