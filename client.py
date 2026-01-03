# client.py
import socket
from config import INDEX_HOST, INDEX_PORT

def ask_index_for_file(file_name):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((INDEX_HOST, INDEX_PORT))

    s.sendall(b"HELLO\n")
    greeting = s.recv(1024).decode().strip()
    print("[CLIENT] Index:", greeting)

    s.sendall(f"GET {file_name}\n".encode())

    resp = s.recv(1024).decode().strip()
    s.close()
    return resp


def download_from_content(ip, port, file_name, out_path):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))

    s.sendall(f"GET {file_name}\n".encode())
    header = s.recv(1024).decode().strip()
    print("[CLIENT] Content header:", header)

    parts = header.split()
    if parts[0] != "OK":
        print("[CLIENT] Error:", header)
        s.close()
        return

    size = int(parts[1])
    remaining = size
    with open(out_path, "wb") as f:
        while remaining > 0:
            data = s.recv(min(4096, remaining))
            if not data:
                break
            f.write(data)
            remaining -= len(data)

    s.close()
    print(f"[CLIENT] Download completed: {out_path}, expected={size}, remaining={remaining}")


def main():
    file_name = input("File name you want: ").strip()

    resp = ask_index_for_file(file_name)
    if resp.startswith("ERROR"):
        print("[CLIENT]", resp)
        return

    # SERVER <ip> <tcp_port> <server_id> <file_size_bytes>
    parts = resp.split()
    if len(parts) != 5 or parts[0] != "SERVER":
        print("[CLIENT] Unexpected response:", resp)
        return

    _, ip, port, server_id, size_hint = parts
    port = int(port)
    print(f"[CLIENT] Index {file_name} provided for {server_id} ({ip}:{port})")

    download_from_content(ip, port, file_name, f"download_{file_name}")


if __name__ == "__main__":
    main()
