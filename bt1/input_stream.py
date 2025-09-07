import socket
import time
import sys

def stream_data(file_path):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 9998))
        s.listen(1)
        print("📡 Server listening on localhost:9998")
        conn, addr = s.accept()
        print(f"✅ Connection from {addr}")
        with conn:
            with open(file_path, "r") as f:
                for line in f:
                    conn.sendall(line.encode("utf-8"))
                    time.sleep(1)

if __name__ == "__main__":
    file_path = sys.argv[1] if len(sys.argv) > 1 else "/app/log/gas_sensor_log.txt"
    stream_data(file_path)
