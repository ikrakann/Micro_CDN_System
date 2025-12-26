# monitor_server.py
import socket
import threading
import time
from config import (
    MONITOR_HOST, MONITOR_UDP_PORT, MONITOR_TCP_PORT,
    HEARTBEAT_TIMEOUT,
    INDEX_HOST, INDEX_PORT
)

# server_id -> info
servers = {}  # {"S1": {"ip": "...", "tcp_port": 7001, "load": 0, "num_files": 3, "last_seen": time, "status": "alive"}}
lock = threading.Lock()


def notify_index_server_down(server_id: str):
    """
    Monitor, server dead tespit edince Index'e proaktif bildirim yollar:
      SERVER_DOWN <server_id> <timestamp>
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5)
        s.connect((INDEX_HOST, INDEX_PORT))
        ts = int(time.time())
        msg = f"SERVER_DOWN {server_id} {ts}\n"
        s.sendall(msg.encode())

        # opsiyonel ack
        _ = s.recv(1024)
        s.close()
        print(f"[MONITOR] Index'e bildirildi: SERVER_DOWN {server_id}")
    except Exception as e:
        print(f"[MONITOR] Index'e SERVER_DOWN gönderilemedi ({server_id}): {e}")


def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((MONITOR_HOST, MONITOR_UDP_PORT))
    print(f"[MONITOR] UDP dinliyor: {MONITOR_HOST}:{MONITOR_UDP_PORT}")

    while True:
        data, addr = sock.recvfrom(1024)
        line = data.decode(errors="ignore").strip()
        parts = line.split()

        # HEARTBEAT <server_id> <ip> <tcp_port> <load> <num_files>
        if len(parts) != 6 or parts[0] != "HEARTBEAT":
            print("[MONITOR] Geçersiz heartbeat:", line)
            continue

        _, server_id, ip, tcp_port, load, num_files = parts
        tcp_port = int(tcp_port)
        load = int(load)
        num_files = int(num_files)

        # Print heartbeat message
        print(f"[MONITOR] Heartbeat from {server_id}: load={load}, files={num_files}")

        with lock:
            # eğer server tekrar heartbeat atmaya başladıysa alive yap
            servers[server_id] = {
                "ip": ip,
                "tcp_port": tcp_port,
                "load": load,
                "num_files": num_files,
                "last_seen": time.time(),
                "status": "alive",
            }


def timeout_checker():
    while True:
        now = time.time()
        to_notify = []

        with lock:
            for sid, info in servers.items():
                if info["status"] == "alive" and now - info["last_seen"] > HEARTBEAT_TIMEOUT:
                    info["status"] = "dead"
                    print(f"[MONITOR] {sid} -> DEAD (timeout)")
                    to_notify.append(sid)

        # lock dışına çıkıp Index'e bildir (bloklamasın)
        for sid in to_notify:
            notify_index_server_down(sid)

        time.sleep(1)


def tcp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((MONITOR_HOST, MONITOR_TCP_PORT))
    sock.listen(5)
    print(f"[MONITOR] TCP dinliyor: {MONITOR_HOST}:{MONITOR_TCP_PORT}")

    while True:
        conn, addr = sock.accept()
        t = threading.Thread(target=handle_tcp_client, args=(conn, addr), daemon=True)
        t.start()


def handle_tcp_client(conn, addr):
    line = conn.recv(1024).decode(errors="ignore").strip()
    if line != "LIST_SERVERS":
        conn.sendall(b"ERROR UNKNOWN_COMMAND\n")
        conn.close()
        return

    with lock:
        for sid, info in servers.items():
            msg = f"SERVER {sid} {info['ip']} {info['tcp_port']} {info['load']} {info['status']}\n"
            conn.sendall(msg.encode())

    conn.sendall(b"END\n")
    conn.close()


def main():
    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=timeout_checker, daemon=True).start()
    threading.Thread(target=tcp_server, daemon=True).start()

    print("[MONITOR] Çalışıyor...")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
