# index_server.py
import socket
import threading
import time
from config import INDEX_HOST, INDEX_PORT, MONITOR_HOST, MONITOR_TCP_PORT

# server_id -> server information
content_servers = {}  # {"S1": {"ip": "127.0.0.1", "tcp_port": 7001, "udp_port": 7002}}

# file_name -> {"size": int, "servers": [server_id, ...]}
file_index = {}  # {"test1.txt": {"size": 12, "servers": ["S1","S2"]}}

# Servers marked as dead by the Index Server
dead_servers = set()

lock = threading.Lock()


# ---------- Getting health information from Monitor (pull) ----------

def get_detailed_status_from_monitor():
    server_status = {}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.5)
        sock.connect((MONITOR_HOST, MONITOR_TCP_PORT))
        sock.sendall(b"LIST_SERVERS\n")

    
        f = sock.makefile("r", encoding="utf-8")
        for line in f:
            line = line.strip()
            if line == "END": break
            parts = line.split()
            if len(parts) >= 6 and parts[0] == "SERVER":
                sid = parts[1]
                load = int(parts[4])
                status = parts[5].lower()
                server_status[sid] = {"load": load, "status": status}
        sock.close()
    except Exception:
        pass
    return server_status

# ---------- Content Server protocol ----------

def handle_content_server(conn, addr, f, first_line):
    """
    Proceeds REGISTER / ADD_FILE / DONE_FILES commands.
    """
    print(f"[INDEX] Content server connected: {addr}")

    def process_line(line: str):
        line = line.strip()
        if not line:
            return

        parts = line.split()
        cmd = parts[0]

        if cmd == "REGISTER":
            # REGISTER <server_id> <server_tcp_port> <server_udp_port>
            if len(parts) != 4:
                conn.sendall(b"ERROR INVALID_REGISTER\n")
                return

            server_id = parts[1]
            tcp_port = int(parts[2])
            udp_port = int(parts[3])

            with lock:
                content_servers[server_id] = {
                    "ip": addr[0],
                    "tcp_port": tcp_port,
                    "udp_port": udp_port,
                }
                # newly registered server is removed from dead set (restart scenario)
                if server_id in dead_servers:
                    dead_servers.remove(server_id)

            print(f"[INDEX] Registered: {server_id} -> {addr[0]}:{tcp_port}")
            conn.sendall(b"OK REGISTERED\n")

        elif cmd == "ADD_FILE":
            # ADD_FILE <server_id> <file_name> <file_size_bytes>
            if len(parts) < 4:
                print("[INDEX] Hatalı ADD_FILE:", parts)
                return

            srv_id = parts[1]
            file_name = parts[2]
            size = int(parts[3])

            with lock:
                if file_name not in file_index:
                    file_index[file_name] = {"size": size, "servers": []}

                # Consider the last one if same file with different size on different servers.
                file_index[file_name]["size"] = size

                if srv_id not in file_index[file_name]["servers"]:
                    file_index[file_name]["servers"].append(srv_id)

            print(f"[INDEX] ADD_FILE: {file_name} ({size} bytes) -> {srv_id}")

        elif cmd == "DONE_FILES":
            conn.sendall(b"OK FILES_ADDED\n")
            print("[INDEX] DONE_FILES received, OK FILES_ADDED sent.")

        else:
            print("[INDEX] Unknown command:", line)

    # ilk satırı işle
    process_line(first_line)

    # sonraki satırlar
    for line in f:
        process_line(line)

    conn.close()
    print(f"[INDEX] Content server connection closed: {addr}")


# ---------- Monitor -> Index (push) SERVER_DOWN ----------

def handle_monitor_push(conn, addr, f, first_line):
    """
    Proactive notifications from Monitor:
      SERVER_DOWN <server_id> <timestamp>
    """
    line = first_line.strip()
    parts = line.split()

    if len(parts) >= 2 and parts[0] == "SERVER_DOWN":
        server_id = parts[1]
        ts = parts[2] if len(parts) >= 3 else str(int(time.time()))

        with lock:
            dead_servers.add(server_id)

        print(f"[INDEX] ALERT: SERVER_DOWN {server_id} ts={ts}")
        conn.sendall(b"OK SERVER_DOWN_RECEIVED\n")
    else:
        conn.sendall(b"ERROR UNKNOWN_COMMAND\n")

    conn.close()


# ---------- Client protocol ----------

def select_content_server_for_file(file_name):
    """
    Servers that host the requested file are identified.
    Alive server information is retrieved from the Monitor and filtered using the Index's dead server set.
    Selection policy: the first suitable server.
    """
    with lock:
        entry = file_index.get(file_name)

    if not entry:
        return None

    server_ids = entry["servers"]
    if not server_ids:
        return None
# Monitor'den tüm sunucuların güncel durumunu (load dahil) alıyoruz
    alive_info = get_detailed_status_from_monitor() 
    
    best_sid = None
    min_load = float('inf')
    file_size = entry["size"]

    with lock:
        for sid in server_ids:
            # 1. Index'in kendi dead listesinde mi?
            if sid in dead_servers:
                continue
            
            # 2. Monitor bu sunucu için 'alive' diyor mu ve yükü ne?
            if sid in alive_info and alive_info[sid]['status'] == 'alive':
                current_load = alive_info[sid]['load']
                
                # En düşük yüklü olanı seç 
                if current_load < min_load:
                    min_load = current_load
                    best_sid = sid

    if best_sid:
        return best_sid, content_servers[best_sid], file_size

    return None


def handle_client(conn, addr, f, first_line):
    print(f"[INDEX] Client connected: {addr}")

    if first_line.strip() != "HELLO":
        conn.sendall(b"ERROR EXPECTED_HELLO\n")
        conn.close()
        return

    conn.sendall(b"WELCOME MICRO-CDN\n")

    line = f.readline()
    if not line:
        conn.close()
        return

    parts = line.strip().split()
    if len(parts) != 2 or parts[0] != "GET":
        conn.sendall(b"ERROR INVALID_COMMAND\n")
        conn.close()
        return

    file_name = parts[1]
    result = select_content_server_for_file(file_name)
    if not result:
        conn.sendall(b"ERROR FILE_NOT_FOUND\n")
        conn.close()
        return

    server_id, info, size = result
    msg = f"SERVER {info['ip']} {info['tcp_port']} {server_id} {size}\n"
    conn.sendall(msg.encode())
    conn.close()
    print(f"[INDEX] For {file_name} , {server_id} selected. size={size}")


# ---------- General connection handler ----------

def connection_handler(conn, addr):
    f = conn.makefile("r", encoding="utf-8", newline="\n")
    first_line = f.readline()
    if not first_line:
        conn.close()
        return

    # Monitor push mesajı
    if first_line.startswith("SERVER_DOWN"):
        handle_monitor_push(conn, addr, f, first_line)
        return

    # Content server
    if first_line.startswith("REGISTER") or first_line.startswith("ADD_FILE"):
        handle_content_server(conn, addr, f, first_line)
        return

    # Client
    handle_client(conn, addr, f, first_line)


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((INDEX_HOST, INDEX_PORT))
    sock.listen(50)
    print(f"[INDEX] Listening on {INDEX_HOST}:{INDEX_PORT}")

    while True:
        conn, addr = sock.accept()
        t = threading.Thread(target=connection_handler, args=(conn, addr), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
