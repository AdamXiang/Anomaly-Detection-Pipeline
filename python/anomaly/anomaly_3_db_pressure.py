"""
Anomaly 3: DB 連線壓力模擬
故事線：80 個 Concurrent Connections 同時寫入 → PG 壓力上升 → 觀察監控
"""
import psycopg2
import threading
import time
import random
from datetime import datetime

# ── 設定 ──────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "sports_iot",
    "user":     "admin",
    "password": "password",
}

NUM_CONNECTIONS  = 80    # 同時連線數
DURATION_SECONDS = 60    # 壓測持續時間
QUERY_INTERVAL   = 0.1   # 每次 query 間隔 (秒)

# ── 統計 ──────────────────────────────────────
stats = {"success": 0, "error": 0, "lock": threading.Lock()}

def worker(thread_id: int, stop_event: threading.Event):
    """模擬一個持續寫入的 DB 連線"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        print(f"  [Thread-{thread_id:02d}] 連線建立 ✓")

        while not stop_event.is_set():
            try:
                # 模擬複雜查詢壓力
                cursor.execute("""
                    SELECT
                        device_id,
                        AVG(heart_rate)    AS avg_hr,
                        AVG(blood_oxygen)  AS avg_bo,
                        COUNT(*)           AS cnt
                    FROM iot_data.athlete_telemetry
                    WHERE event_time > NOW() - INTERVAL '5 minutes'
                    GROUP BY device_id
                    ORDER BY cnt DESC
                    LIMIT 10;
                """)
                cursor.fetchall()

                with stats["lock"]:
                    stats["success"] += 1

                time.sleep(QUERY_INTERVAL + random.uniform(0, 0.05))

            except Exception as e:
                with stats["lock"]:
                    stats["error"] += 1

    except Exception as e:
        print(f"  [Thread-{thread_id:02d}] 連線失敗: {e}")
        with stats["lock"]:
            stats["error"] += 1
    finally:
        if conn:
            conn.close()


def main():
    print("=" * 50)
    print("  Anomaly 3: DB 連線壓力模擬")
    print("=" * 50)
    print(f"\n  目標連線數:  {NUM_CONNECTIONS}")
    print(f"  壓測時間:    {DURATION_SECONDS} 秒")
    print(f"\n  請開啟 Grafana 觀察:")
    print("  http://localhost:3000/d/iot-infra-002")
    print("  → PG Active Connections 應快速上升 📈")
    print("  → PG DB Size 持續增長 📈\n")

    input("  按 Enter 開始壓測...")

    stop_event = threading.Event()
    threads    = []

    # ── 啟動所有 Worker ──
    print(f"\n  [ACTION] 啟動 {NUM_CONNECTIONS} 個並發連線...")
    for i in range(NUM_CONNECTIONS):
        t = threading.Thread(target=worker, args=(i, stop_event), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.02)  # 錯開啟動，避免瞬間 connection storm

    print(f"  [INFO] 全部連線已啟動，開始 {DURATION_SECONDS} 秒壓測\n")

    # ── 監控進度 ──
    start_time = time.time()
    while time.time() - start_time < DURATION_SECONDS:
        elapsed = int(time.time() - start_time)
        with stats["lock"]:
            s, e = stats["success"], stats["error"]
        print(
            f"  [{elapsed:3d}s] "
            f"成功查詢: {s:6d} | "
            f"錯誤: {e:4d} | "
            f"QPS ≈ {s/(elapsed+1):6.1f}"
        )
        time.sleep(5)

    # ── 停止壓測 ──
    print("\n  [RECOVER] 停止壓測，關閉所有連線...")
    stop_event.set()
    for t in threads:
        t.join(timeout=3)

    with stats["lock"]:
        s, e = stats["success"], stats["error"]

    print("\n" + "=" * 50)
    print("  Anomaly 3 完成！結果摘要：")
    print(f"    總成功查詢: {s:,}")
    print(f"    總錯誤數:   {e:,}")
    print(f"    平均 QPS:   {s/DURATION_SECONDS:.1f}")
    print("  故事線：")
    print("    PG Connections 飆升 → 觀察監控 → 壓測結束恢復 ✅")
    print("=" * 50)


if __name__ == "__main__":
    main()
