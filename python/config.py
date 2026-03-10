import os
from pathlib import Path
from dotenv import load_dotenv


env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path, verbose=True)

try:
    NUM_DEVICES = os.environ['NUM_DEVICES']
    print('NUM_DEVICES =', NUM_DEVICES)

    if NUM_DEVICES is None:
        raise ValueError('NUM_DEVICES environment variable is not set')
except KeyError:
    raise

class KafkaConfig:
    BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092,localhost:9093,localhost:9094"  # 對應 docker-compose 的 external port
    )
    TOPIC_NAME: str = os.getenv("KAFKA_TOPIC", "watch-telemetry")
    NUM_PARTITIONS: int = 3
    REPLICATION_FACTOR: int = 3

    # Producer 設定（偏向高可靠）
    # ================================================================
    # 1. 達成效果：最高級別的資料寫入保證（High Durability）。
    # 2. 解決問題：解決 Kafka 叢集單一節點（Broker）突然當機時，資料遺失的問題。acks="all" 要求資料必須成功寫入 Leader，且同步複製到其他 Follower 後，才回報「寫入成功」。
    # 3. 不配置的後果：預設值是 acks=1（只要 Leader 收到就算成功）。如果 Leader 剛收到資料還沒同步給 Follower 就突然斷電當機，這筆心跳或 GPS 資料就永遠消失了。
    # ================================================================

    # batch.size: 16384 (16KB) 與 linger.ms: 10 (10毫秒)
    # ================================================================
    # 1. 達成效果：大幅提升系統吞吐量（Throughput），降低網路 I/O 負擔。
    # 2. 解決問題：IoT 設備的資料通常很小（可能只有幾十 Bytes），如果每一筆都單獨發送一次網路請求，網路頻寬和 Kafka 會被龐大的連線開銷（Overhead）壓垮。這兩個參數就像「公車發車規則」：只要乘客滿 16KB（batch.size）就發車，或者就算沒滿，乘客等了 10 毫秒（linger.ms）也必須發車。
    # 3. 不配置的後果：如果不設 linger.ms（預設為 0），公車一有乘客就立刻開走。在幾萬支手錶同時上傳資料的「爆量場景」下，會產生無數個微小的網路請求，導致 Kafka CPU 飆高、網路壅塞，甚至引發延遲災難。
    # ================================================================
    PRODUCER_CONFIG: dict = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",                  # 等待所有 ISR replica 確認
        "retries": 5,                   # 失敗最多重試 5 次
        "retry.backoff.ms": 500,        # 每次重試間隔 500ms
        "batch.size": 16384,            # 每個 batch 最大 16KB
        "linger.ms": 10,                # 等 10ms 湊批（爆量場景優化）
        "compression.type": "lz4",      # LZ4 壓縮（速度快，減少網路傳輸）
        "enable.idempotence": True,     # 冪等性：避免 Producer retry 造成重複
    }

    # Consumer 設定
    # ================================================================
    # enable.auto.commit: False
    # 1. 達成效果：把「確認這筆資料已經處理完」的控制權，交給你的 Python 程式，而不是讓 Kafka 自動倒數計時。這實現了 At-least-once 語意。
    # 2. 解決問題：防止資料在「處理中」遺失。標準流程變成：從 Kafka 拿資料 ➔ 運算 ➔ 成功寫入 Postgres ➔ 最後才手動 Commit (打勾確認)。
    # 3. 不配置的後果：預設是 True。假設你的 Consumer 抓了 100 筆資料，Kafka 就自動標記這 100 筆為「已讀」。此時如果你的 Python 程式突然當機（還沒寫進資料庫），重啟後 Kafka 會從第 101 筆開始給資料，中間那 100 筆資料就徹底人間蒸發了。

    # auto.offset.reset: "earliest"
    # 1. 達成效果：當一個全新的 Consumer Group 加入，或者找不到歷史讀取進度時，從 Kafka 裡最舊的資料開始讀。
    # 2. 解決問題：確保系統第一次上線，或是換了新的 group.id 時，能夠把 Kafka 裡面暫存的歷史資料全部消化完，不會漏掉上線前的數據。
    # 3. 不配置的後果：預設是 latest（從最新的一筆開始讀）。如果你的系統暫停服務了 10 分鐘，重新啟動後，這 10 分鐘內手錶上傳的資料都會被跳過，直接被捨棄。
    # ================================================================
    CONSUMER_CONFIG: dict = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "iot-consumer-group",
        "auto.offset.reset": "earliest",            # 從最早的訊息開始消費
        "enable.auto.commit": False,                # 關閉自動 commit（手動控制）
        "max.poll.interval.ms": 300000,             # 5 分鐘內必須 poll 一次
        "session.timeout.ms": 30000,                # 30 秒沒心跳視為 Consumer 死亡
    }


class PostgresConfig:
    HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    DATABASE: str = os.getenv("POSTGRES_DB", "sports_iot")
    USER: str = os.getenv("POSTGRES_USER", "admin")
    PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")

    # DB 設置
    # ================================================================
    # BATCH_SIZE = 500 與 BATCH_TIMEOUT_SEC = 5
    # 1. 達成效果：降低資料庫連線壓力，同時保證資料不會在記憶體裡卡太久。
    # 2. 解決問題：
    #          A. 效能問題：資料庫最怕的是「一筆一筆 Insert」。透過收集 500 筆再一次性打包寫入（Bulk Insert），效能可以提升幾十倍。
    #          B. 低流量卡死問題：如果只看數量（湊滿 500 筆才寫入），在半夜沒有人在運動時，可能要等 1 個小時才會湊滿 500 筆。加上 TIMEOUT=5，代表只要過了 5 秒，就算只有 3 筆資料，也會強制寫入資料庫。
    # 3. 不配置的後果：
    #          A. 如果不批次 (無 BATCH_SIZE)：每秒幾千次的 Insert 會瞬間榨乾 Postgres 的 CPU 與硬碟 IOPS，導致資料庫卡死。
    #          B. 如果沒有超時機制 (無 BATCH_TIMEOUT)：系統在離峰時段會出現嚴重的「資料延遲」，使用者在 App 上看手錶數據會發現資料過了半小時都還沒更新，因為資料還卡在 Python 的記憶體裡等著湊滿 500 筆。
    # ================================================================

    BATCH_SIZE = 500  # 每批寫入 500 筆（平衡效能與延遲）
    BATCH_TIMEOUT_SEC = 5  # 最多等 5 秒就強制寫入（低流量時不讓資料卡住）

    @classmethod
    def get_dsn(cls) -> str:
        """回傳 PostgreSQL DSN 連線字串"""
        return (
            f"host={cls.HOST} "
            f"port={cls.PORT} "
            f"dbname={cls.DATABASE} "
            f"user={cls.USER} "
            f"password={cls.PASSWORD}"
        )


# ─────────────────────────────────────
# 模擬器設定
# ─────────────────────────────────────
class SimulatorConfig:
    # 裝置數量
    NUM_DEVICES: int = int(os.getenv("NUM_DEVICES", "50"))

    # 正常模式
    NORMAL_RATE_PER_SEC: int = 100  # 100 msg/sec

    # 爆量模式（馬拉松起跑）
    BURST_RATE_PER_SEC: int = 10_000  # 10,000 msg/sec
    BURST_DURATION_SEC: int = 60  # 持續 60 秒

