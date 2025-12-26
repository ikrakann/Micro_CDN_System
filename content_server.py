# content_server.py
import socket
import threading
import os
import time
import sys
from config import INDEX_HOST, INDEX_PORT, MONITOR_HOST, MONITOR_UDP_PORT, HEARTBEAT_INTERVAL

def register_with_index(server_id, tcp_port, udp_port, files_dir):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((INDEX_HOST, INDEX_PORT))

    # REGISTER
    msg = f"REGISTER {server_id} {tcp_port} {udp_port}\n"
    s.sendall(msg.encode())
    print(f"[{server_id}] REGISTER gönderildi.")
    print(s.recv(1024).decode().strip())

    # dosya listesi
    for fname in os.listdir(files_dir):
        path = os.path.join(files_dir, fname)
        if os.path.isfile(path):
            size = os.path.getsize(path)
            line = f"ADD_FILE {server_id} {fname} {size}\n"
            s.sendall(line.encode())

    s.sendall(b"DONE_FILES\n")
    print(s.recv(1024).decode().strip())
    s.close()


def handle_client(conn, addr, files_dir, server_id):
    print(f"[{server_id}] Client bağlandı: {addr}")
    line = conn.recv(1024).decode()
    parts = line.strip().split()
    if len(parts) != 2 or parts[0] != "GET":
        conn.sendall(b"ERROR INVALID_COMMAND\n")
        conn.close()
        return

    file_name = parts[1]
    path = os.path.join(files_dir, file_name)

    if not os.path.exists(path):
        conn.sendall(b"ERROR FILE_NOT_FOUND\n")
        conn.close()
        return

    size = os.path.getsize(path)
    header = f"OK {size}\n"
    conn.sendall(header.encode())

    with open(path, "rb") as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            conn.sendall(data)

    conn.close()
    print(f"[{server_id}] {file_name} gönderildi.")


def tcp_server(server_id, tcp_port, files_dir):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0", tcp_port))
    sock.listen(5)
    print(f"[{server_id}] TCP port {tcp_port} dinleniyor")

    while True:
        conn, addr = sock.accept()
        t = threading.Thread(target=handle_client,
                             args=(conn, addr, files_dir, server_id),
                             daemon=True)
        t.start()


def heartbeat_sender(server_id, tcp_port, files_dir, udp_port):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        # load = aktif client sayısı gibi geliştirilebilir, şimdilik 0
        load = 0
        num_files = len([f for f in os.listdir(files_dir)
                         if os.path.isfile(os.path.join(files_dir, f))])

        msg = f"HEARTBEAT {server_id} 127.0.0.1 {tcp_port} {load} {num_files}\n"
        udp.sendto(msg.encode(), (MONITOR_HOST, MONITOR_UDP_PORT))
        time.sleep(HEARTBEAT_INTERVAL)


def main():
    if len(sys.argv) != 5:
        print("Kullanım: python content_server.py <SERVER_ID> <TCP_PORT> <UDP_PORT> <FILES_DIR>")
        sys.exit(1)

    server_id = sys.argv[1]
    tcp_port = int(sys.argv[2])
    udp_port = int(sys.argv[3])
    files_dir = sys.argv[4]

    os.makedirs(files_dir, exist_ok=True)

    register_with_index(server_id, tcp_port, udp_port, files_dir)

    t1 = threading.Thread(target=tcp_server, args=(server_id, tcp_port, files_dir), daemon=True)
    t1.start()

    t2 = threading.Thread(target=heartbeat_sender,
                          args=(server_id, tcp_port, files_dir, udp_port),
                          daemon=True)
    t2.start()

    print(f"[{server_id}] Content Server çalışıyor...")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
