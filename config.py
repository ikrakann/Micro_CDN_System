# config.py

# Tüm bileşenler localhost üzerinde çalışacak
INDEX_HOST = "127.0.0.1"
INDEX_PORT = 5000

MONITOR_HOST = "127.0.0.1"
MONITOR_UDP_PORT = 6000
MONITOR_TCP_PORT = 6001

HEARTBEAT_INTERVAL = 3      # saniye
HEARTBEAT_TIMEOUT = 8       # saniye (bu süreyi aşarsa dead)
