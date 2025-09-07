# 📖 IoT Spark Streaming Project

## 🚀 Giới thiệu  
Project này mô phỏng hệ thống IoT sử dụng **Apache Spark Streaming** để xử lý dữ liệu cảm biến theo thời gian thực.  
Nguồn dữ liệu được giả lập qua **socket server** (`input_stream.py`) đọc từ file log, gửi từng dòng vào Spark Streaming (`main.py`) để xử lý, phân tích và lưu vào **MongoDB**.  

Có **3 bài tập chính**:  

---

## 🔹 Bài 1 – Gas Sensor (Cảm biến khí Gas)  

### Mô tả  
- Giả lập cảm biến khí gas trong nhà/máy móc.  
- Nếu nồng độ khí vượt ngưỡng an toàn → tạo cảnh báo.  
- Spark Streaming dùng `foreachRDD` để ghi dữ liệu và cảnh báo vào MongoDB.  

### Chạy thử  
Mở 1 terminal chạy socket server (giả lập log gas sensor)
```bash
docker exec -it spark-master spark-submit /app/bt1/input_stream.py
```
<img width="1920" height="1080" alt="07-09-2025screenshot-13-54-40-890" src="https://github.com/user-attachments/assets/35280681-1bb1-4cfa-ade1-b200b17bffa5" />

Mở terminal khác chạy Spark Streaming xử lý log
```bash
docker exec -it spark-master spark-submit /app/bt1/main.py
```
<img width="1920" height="1080" alt="07-09-2025screenshot-13-53-58-783" src="https://github.com/user-attachments/assets/93075230-9403-4c12-901f-988928be667f" />
<img width="1920" height="1080" alt="07-09-2025screenshot-13-54-09-256" src="https://github.com/user-attachments/assets/f8033b54-74f4-43f2-aef0-d9a0a4036733" />


## 🔹 Bài 2 – Robot Vacuum (Máy hút bụi thông minh)  

### Mô tả  
- Giả lập **10 robot hút bụi** gửi dữ liệu mỗi giây gồm:  
  - `robot_id`: mã định danh robot  
  - `pin`: mức pin còn lại (%)  
  - `x, y`: tọa độ di chuyển trong không gian  
  - `stuck_flag`: trạng thái kẹt (0 = bình thường, 1 = bị kẹt)  
- Spark Streaming xử lý log bằng:  
  - `foreachRDD`: ghi dữ liệu thô và cảnh báo vào MongoDB.  
  - `window` + `reduceByKey`:  
    - Thống kê robot nào bị **pin thấp nhiều nhất** trong 10 phút gần nhất.  
    - Tính **tổng số lần robot bị kẹt** để phục vụ bảo trì.  

### Chạy thử (demo 20 giây)  
```bash
# Mở 1 terminal chạy socket server (giả lập log robot)
docker exec -it spark-master spark-submit /app/bt2/input_stream.py

# Mở terminal khác chạy Spark Streaming xử lý log robot
docker exec -it spark-master spark-submit /app/bt2/main.py
```
