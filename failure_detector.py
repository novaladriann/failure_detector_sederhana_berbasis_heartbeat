"""
Failure Detector sederhana berbasis Heartbeat (UDP, localhost).

Mekanisme:
  - Setiap node mengirim heartbeat periodik ke failure detector.
  - Jika sebuah node tidak mengirim heartbeat lebih lama dari TIMEOUT,
    detector menandainya SUSPECTED (dicurigai gagal).
  - Jika heartbeat datang lagi, node ditandai ALIVE kembali (RECOVER).

Cara menjalankan:
    python failure_detector.py
Opsional (mengubah timeout untuk eksperimen false positive):
    python failure_detector.py 1.2
"""
import json
import socket
import sys
import threading
import time

HB_INTERVAL = 1.0   # node mengirim heartbeat tiap 1 detik
TIMEOUT = 3.0       # dianggap gagal jika > 3 detik tanpa heartbeat
PORT = 9000
RUN_SECONDS = 16    # lama simulasi


def log(tag, msg, t0):
    """Cetak log dengan penanda waktu relatif sejak simulasi dimulai."""
    print(f"[{time.time() - t0:5.1f}s] [{tag}] {msg}", flush=True)


class FailureDetector:
    def __init__(self, t0, port=PORT, timeout=TIMEOUT):
        self.t0 = t0
        self.timeout = timeout
        self.last_seen = {}   # node_id -> waktu heartbeat terakhir
        self.status = {}      # node_id -> "ALIVE" / "SUSPECTED"
        self.lock = threading.Lock()
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.5)

    def listen(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(1024)
            except socket.timeout:
                continue
            nid = json.loads(data.decode())["id"]
            with self.lock:
                if nid not in self.status:
                    log("JOIN", f"{nid} terdaftar", self.t0)
                elif self.status[nid] == "SUSPECTED":
                    log("RECOVER", f"{nid} aktif kembali", self.t0)
                self.last_seen[nid] = time.time()
                self.status[nid] = "ALIVE"

    def check(self):
        while self.running:
            now = time.time()
            with self.lock:
                for nid, seen in self.last_seen.items():
                    silent = now - seen
                    if silent > self.timeout and self.status[nid] == "ALIVE":
                        self.status[nid] = "SUSPECTED"
                        log("SUSPECT",
                            f"{nid} tanpa heartbeat selama {silent:.1f}s",
                            self.t0)
            time.sleep(0.5)

    def stop(self):
        self.running = False


def node(nid, port=PORT, crash_at=None, back_at=None):
    """
    Simulasi sebuah node.
    crash_at : detik ke-berapa node berhenti mengirim heartbeat (crash)
    back_at  : detik ke-berapa node pulih dan mengirim heartbeat lagi
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start = time.time()
    while time.time() - start < RUN_SECONDS:
        elapsed = time.time() - start
        crashed = (crash_at is not None and elapsed >= crash_at
                   and (back_at is None or elapsed < back_at))
        if not crashed:
            msg = json.dumps({"id": nid, "ts": time.time()}).encode()
            s.sendto(msg, ("127.0.0.1", port))
        time.sleep(HB_INTERVAL)


def main():
    timeout = float(sys.argv[1]) if len(sys.argv) > 1 else TIMEOUT
    t0 = time.time()
    print(f"Simulasi dimulai (HB_INTERVAL={HB_INTERVAL}s, TIMEOUT={timeout}s)")

    fd = FailureDetector(t0, timeout=timeout)
    threading.Thread(target=fd.listen, daemon=True).start()
    threading.Thread(target=fd.check, daemon=True).start()

    # node-A dan node-B normal; node-C crash di detik 5 dan pulih di detik 11
    threading.Thread(target=node, args=("node-A",), daemon=True).start()
    threading.Thread(target=node, args=("node-B",), daemon=True).start()
    threading.Thread(target=node, args=("node-C", PORT, 5, 11),
                     daemon=True).start()

    time.sleep(RUN_SECONDS + 1)
    fd.stop()

    print("\nStatus akhir:")
    for nid, st in sorted(fd.status.items()):
        print(f"  {nid}: {st}")


if __name__ == "__main__":
    main()
    
    
