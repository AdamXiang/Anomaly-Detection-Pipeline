# python/pipeline/consumer.py

import json
import signal
import sys
import time
from dataclasses import dataclass
from typing import Optional

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from loguru import logger

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

from config import KafkaConfig, PostgresConfig
from utils.logger import setup_logger


# ─────────────────────────────────────────────────────────────────────────────
# Data Model（對應 Kafka 訊息格式）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TelemetryRecord:
    """
    從 Kafka 解析出來的一筆遙測資料。
    對應 init.sql 的 iot_data.watch_telemetry table。
    """
    device_id:     str
    user_id:       str
    event_time:    str
    heart_rate:    int
    blood_oxygen:  float
    steps:         int
    battery_level: int
    latitude:      float
    longitude:     float
    mode:          str
    raw_offset:    int       # 記錄這筆資料來自哪個 Kafka Offset（方便 debug）
    raw_partition: int       # 記錄來自哪個 Partition


# ─────────────────────────────────────────────────────────────────────────────
# Batch Stats（追蹤每個 Micro-batch 的統計）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BatchStats:
    batch_no:     int   = 0
    total_insert: int   = 0
    total_error:  int   = 0

    def next_batch(self) -> None:
        self.batch_no += 1

    def record_insert(self, n: int) -> None:
        self.total_insert += n

    def record_error(self, n: int) -> None:
        self.total_error += n


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL Writer
# ─────────────────────────────────────────────────────────────────────────────

class PostgresWriter:
    """
    負責將 TelemetryRecord 批次寫入 PostgreSQL。

    設計重點：
      - 用 execute_values() 做 Bulk Insert（比逐筆 INSERT 快 10-50 倍）
      - 用 ON CONFLICT DO NOTHING 實現 Idempotent（冪等）寫入
        → 即使 Consumer 重啟後重複消費，也不會產生重複資料
      - 連線失敗時自動重試（最多 3 次）
    """

    INSERT_SQL = """
                 INSERT INTO iot_data.athlete_telemetry (device_id, user_id, event_time, \
                                                         heart_rate, blood_oxygen, steps, \
                                                         battery_level, latitude, longitude, mode)
                 VALUES %s ON CONFLICT DO NOTHING \
    """
    # ↑ ON CONFLICT DO NOTHING：
    #   如果同一筆資料（device_id + event_time 組合）已經存在，
    #   就靜默忽略，不拋錯誤。
    #   這需要 init.sql 裡有對應的 UNIQUE 約束。

    def __init__(self):
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._connect()

    def _connect(self) -> None:
        """建立 PostgreSQL 連線，失敗時最多重試 3 次。"""
        for attempt in range(1, 4):
            try:
                self._conn = psycopg2.connect(PostgresConfig.get_dsn())
                self._conn.autocommit = False  # 手動控制 Transaction
                logger.info(f"✅ PostgreSQL connected (attempt {attempt})")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"⚠️  PostgreSQL connect failed (attempt {attempt}/3): {e}")
                if attempt < 3:
                    time.sleep(2 ** attempt)  # Exponential backoff: 2s, 4s
                else:
                    logger.critical("❌ PostgreSQL connection failed after 3 attempts.")
                    raise

    def write_batch(self, records: list[TelemetryRecord]) -> tuple[int, int]:
        """
        批次寫入一組 TelemetryRecord 到 PostgreSQL。

        Returns:
            (inserted_count, error_count)

        Transaction 策略：
          - 整個 batch 是一個 Transaction
          - 全部成功 → COMMIT
          - 任何失敗 → ROLLBACK，回傳 error_count = len(records)

        為什麼不逐筆 commit？
          → 每次 COMMIT 都是一次 fsync，1000 筆個別 commit
            比 1000 筆一起 commit 慢 100 倍以上。
        """
        if not records:
            return 0, 0

        values = [
            (
                r.device_id,
                r.user_id,  # ← 新增
                r.event_time,
                r.heart_rate,
                r.blood_oxygen,
                r.steps,  # ← 原本是 step_count，現在對應 steps
                r.battery_level,
                r.latitude,
                r.longitude,
                r.mode,  # ← 新增
            )
            for r in records
        ]

        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    self.INSERT_SQL,
                    values,
                    page_size=500,  # 每次最多 500 筆一個 SQL 語句
                )
                inserted = cur.rowcount
                self._conn.commit()

                logger.debug(
                    f"💾 DB write success | "
                    f"records={len(records)} | "
                    f"inserted={inserted}"
                )
                return inserted, 0

        except psycopg2.Error as e:
            # 寫入失敗 → 整個 batch rollback
            self._conn.rollback()
            logger.error(
                f"❌ DB write failed | "
                f"records={len(records)} | "
                f"error={e}"
            )
            # 嘗試重連（可能是連線中斷）
            try:
                self._connect()
            except Exception:
                pass
            return 0, len(records)

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("✅ PostgreSQL connection closed.")


# ─────────────────────────────────────────────────────────────────────────────
# IoT Kafka Consumer（核心）
# ─────────────────────────────────────────────────────────────────────────────

class IoTConsumer:
    """
    從 Kafka 消費 IoT 遙測資料，批次寫入 PostgreSQL。

    Micro-batch 策略：
      - 條件 1：累積到 BATCH_SIZE 筆（預設 500）
      - 條件 2：距上次寫入超過 BATCH_TIMEOUT_SEC 秒（預設 5）
      → 兩個條件任一滿足就立刻寫入，避免低流量時資料卡在記憶體太久。

    手動 Commit 策略：
      - 先寫入 PostgreSQL → 成功後才 commit Kafka offset
      - 如果寫入 DB 失敗，不 commit → Consumer 重啟後會重新消費
      - 這是 At-Least-Once 語義，搭配 ON CONFLICT DO NOTHING 達到最終一致
    """

    def __init__(self):
        self._consumer = self._create_consumer()
        self._db = PostgresWriter()
        self._stats = BatchStats()
        self._running = False

        # Micro-batch buffer
        self._buffer: list[TelemetryRecord] = []
        self._last_flush_time: float = time.time()

        # Graceful Shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        logger.info(
            f"IoTConsumer initialized | "
            f"topic={KafkaConfig.TOPIC_NAME} | "
            f"batch_size={PostgresConfig.BATCH_SIZE} | "
            f"batch_timeout={PostgresConfig.BATCH_TIMEOUT_SEC}s"
        )

    # ── 私有方法 ──────────────────────────────────────────────────────────────

    def _create_consumer(self) -> Consumer:
        """建立 confluent-kafka Consumer。"""
        try:
            consumer = Consumer(KafkaConfig.CONSUMER_CONFIG)
            consumer.subscribe(
                [KafkaConfig.TOPIC_NAME],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            logger.info("✅ Kafka Consumer created and subscribed.")
            return consumer
        except KafkaException as e:
            logger.critical(f"❌ Failed to create Kafka Consumer: {e}")
            sys.exit(1)

    def _on_assign(self, consumer, partitions) -> None:
        """Rebalance 時，被分配到新 Partition 的 callback。"""
        logger.info(
            f"📌 Partitions assigned: "
            f"{[f'p{p.partition}' for p in partitions]}"
        )

    def _on_revoke(self, consumer, partitions) -> None:
        """
        Rebalance 時，被撤銷 Partition 前的 callback。
        重要：在這裡 flush 當前 buffer，避免資料遺失。
        """
        logger.warning(
            f"⚠️  Partitions revoked: "
            f"{[f'p{p.partition}' for p in partitions]} "
            f"→ flushing buffer..."
        )
        self._flush_buffer()

    def _parse_message(self, msg) -> Optional[TelemetryRecord]:
        """
        解析 Kafka 訊息 → TelemetryRecord。
        解析失敗時回傳 None（讓呼叫端決定如何處理）。
        """
        try:
            data = json.loads(msg.value().decode("utf-8"))
            return TelemetryRecord(
                device_id=data["device_id"],
                user_id=data["user_id"],
                event_time=data["event_time"],
                heart_rate=int(data["heart_rate"]),
                blood_oxygen=float(data["blood_oxygen"]),
                steps=int(data["steps"]),
                battery_level=int(data["battery_level"]),
                latitude=float(data["latitude"]),
                longitude=float(data["longitude"]),
                mode=data["mode"],
                raw_offset=msg.offset(),
                raw_partition=msg.partition(),
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(
                f"⚠️  Parse failed | "
                f"offset={msg.offset()} | "
                f"partition={msg.partition()} | "
                f"error={e} | "
                f"raw={msg.value()[:100]}"   # 只印前 100 bytes
            )
            return None

    def _should_flush(self) -> bool:
        """
        判斷是否要觸發 flush（寫入 DB）。
        條件 1：buffer 達到 BATCH_SIZE
        條件 2：距上次 flush 超過 BATCH_TIMEOUT_SEC 秒
        """
        if len(self._buffer) >= PostgresConfig.BATCH_SIZE:
            return True
        elapsed = time.time() - self._last_flush_time
        if elapsed >= PostgresConfig.BATCH_TIMEOUT_SEC and self._buffer:
            return True
        return False

    def _flush_buffer(self) -> None:
        """
        將 buffer 內的資料寫入 PostgreSQL，
        成功後才 commit Kafka offset。

        這是整個系統最關鍵的設計：
          DB write → commit offset（順序不能反）
          如果先 commit 再寫 DB，DB 失敗後訊息就永遠消失了。
        """
        if not self._buffer:
            return

        batch_size = len(self._buffer)
        self._stats.next_batch()

        logger.info(
            f"💾 Flushing batch #{self._stats.batch_no} | "
            f"size={batch_size} | "
            f"buffer_age={time.time() - self._last_flush_time:.1f}s"
        )

        # Step 1：寫入 PostgreSQL
        inserted, errors = self._db.write_batch(self._buffer)
        self._stats.record_insert(inserted)
        self._stats.record_error(errors)

        if errors == 0:
            # Step 2：DB 成功 → commit Kafka offset
            try:
                self._consumer.commit(asynchronous=False)  # 同步 commit，確保 commit 完成
                logger.info(
                    f"✅ Batch #{self._stats.batch_no} done | "
                    f"inserted={inserted} | "
                    f"total_insert={self._stats.total_insert:,} | "
                    f"total_error={self._stats.total_error:,}"
                )
            except KafkaException as e:
                logger.error(f"❌ Kafka commit failed: {e}")
        else:
            # Step 3：DB 失敗 → 不 commit，讓 Consumer 重啟後重消費
            logger.error(
                f"❌ Batch #{self._stats.batch_no} DB write failed | "
                f"errors={errors} | "
                f"⚠️  Offset NOT committed → will retry on restart"
            )

        # 清空 buffer，重置計時器
        self._buffer.clear()
        self._last_flush_time = time.time()

    def _signal_handler(self, signum, frame) -> None:
        logger.warning(f"⛔ Signal {signum} received → graceful shutdown...")
        self._running = False

    # ── 公開方法 ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        主迴圈：持續從 Kafka poll 訊息，填入 buffer，
        達到條件就 flush。
        """
        self._running = True
        logger.info("🚀 IoTConsumer started.")

        try:
            while self._running:
                # poll(timeout)：等待最多 1 秒
                # 如果沒有新訊息，回傳 None（不阻塞）
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # 沒有新訊息，檢查是否需要 timeout flush
                    if self._should_flush():
                        self._flush_buffer()
                    continue

                if msg.error():
                    # Kafka 錯誤處理
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 讀到 Partition 尾端（正常現象，表示目前沒有新資料）
                        logger.debug(
                            f"📭 Reached end of partition | "
                            f"partition={msg.partition()} | "
                            f"offset={msg.offset()}"
                        )
                    else:
                        logger.error(f"❌ Kafka error: {msg.error()}")
                    continue

                # 解析訊息
                record = self._parse_message(msg)
                if record:
                    self._buffer.append(record)

                # 判斷是否需要 flush
                if self._should_flush():
                    self._flush_buffer()

        except Exception as e:
            logger.critical(f"💥 Unexpected error: {e}", exc_info=True)

        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """
        優雅關閉：
        1. flush 剩餘 buffer
        2. close Consumer（觸發 Rebalance）
        3. close DB 連線
        4. 印出最終統計
        """
        logger.info("🔄 Shutting down → flushing remaining buffer...")

        # 最後一次 flush
        self._flush_buffer()

        # 關閉 Consumer
        self._consumer.close()
        logger.info("✅ Kafka Consumer closed.")

        # 關閉 DB
        self._db.close()

        # 最終統計
        logger.info(
            f"📋 Final Stats | "
            f"batches={self._stats.batch_no} | "
            f"total_insert={self._stats.total_insert:,} | "
            f"total_error={self._stats.total_error:,}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logger("iot-consumer")

    print("=" * 60)
    print("🧪 IoTConsumer — 啟動")
    print("   從 Kafka 消費資料，批次寫入 PostgreSQL")
    print("   按 Ctrl+C 優雅關閉")
    print("=" * 60)

    consumer = IoTConsumer()
    consumer.run()
