# python/pipeline/producer.py

import json
import signal
import sys
import time
import threading
from typing import Optional

from confluent_kafka import Producer, KafkaException
from loguru import logger

# 將 python/ 加入 path（從任何地方執行都能正確 import）
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

from config import KafkaConfig, SimulatorConfig
from generator.data_generator import IoTDataGenerator, SimulationMode
from utils.logger import setup_logger


# ─────────────────────────────────────────────────────────────────────────────
# Delivery Callback
# ─────────────────────────────────────────────────────────────────────────────

class DeliveryTracker:
    """
    追蹤 Producer 的送達狀況。
    confluent-kafka 的 Delivery Callback 是在 poll() 時觸發，
    所以需要一個共享狀態來跨執行緒統計。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.total_sent: int = 0
        self.total_failed: int = 0
        self._last_report_time: float = time.time()
        self._messages_since_last_report: int = 0

    def on_delivery(self, err, msg) -> None:
        """
        Delivery Callback（confluent-kafka 要求的介面）。
        ⚠️ 這個函式是在 Producer.poll() 的執行緒中被呼叫，
           所以對共享狀態的操作必須是 thread-safe。

        Args:
            err: None 代表成功；KafkaError 代表失敗
            msg: 送出的訊息物件
        """
        with self._lock:
            if err is not None:
                # 送達失敗（Broker 拒絕、網路問題等）
                self.total_failed += 1
                logger.error(
                    f"❌ Delivery FAILED | "
                    f"topic={msg.topic()} | "
                    f"partition={msg.partition()} | "
                    f"error={err}"
                )
            else:
                # 送達成功
                self.total_sent += 1
                self._messages_since_last_report += 1

                # 每 1000 筆 log 一次（避免 log 太多）
                if self.total_sent % 1000 == 0:
                    logger.debug(
                        f"✅ Delivered #{self.total_sent:,} | "
                        f"topic={msg.topic()} | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()}"
                    )

    def throughput_report(self) -> Optional[str]:
        """
        計算並回傳吞吐量報告字串。
        每次呼叫會重置計數器（用於週期性 log）。
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_report_time

            if elapsed < 1.0:
                return None

            rate = self._messages_since_last_report / elapsed
            report = (
                f"📊 Throughput | "
                f"rate={rate:,.0f} msg/sec | "
                f"total_sent={self.total_sent:,} | "
                f"total_failed={self.total_failed:,}"
            )

            # 重置
            self._last_report_time = now
            self._messages_since_last_report = 0

            return report


# ─────────────────────────────────────────────────────────────────────────────
# IoT Kafka Producer
# ─────────────────────────────────────────────────────────────────────────────

class IoTProducer:
    """
    封裝 confluent-kafka Producer，
    將 IoT 遙測資料送往 Kafka Topic。

    使用方式：
        producer = IoTProducer()
        producer.run()            # 正常模式（阻塞式）

        # 另一個執行緒觸發爆量
        producer.trigger_burst(duration_sec=60)
    """

    def __init__(self):
        self._tracker = DeliveryTracker()
        self._producer = self._create_producer()
        self._generator = IoTDataGenerator(
            num_devices=SimulatorConfig.NUM_DEVICES
        )
        self._running = False
        self._topic = KafkaConfig.TOPIC_NAME

        # 設定 Graceful Shutdown 的信號處理
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        logger.info(
            f"IoTProducer initialized | "
            f"topic={self._topic} | "
            f"brokers={KafkaConfig.BOOTSTRAP_SERVERS}"
        )

    # ── 私有方法 ──────────────────────────────────────────────────────────────

    def _create_producer(self) -> Producer:
        """
        建立 confluent-kafka Producer。
        設定說明請參考 config.py 的 PRODUCER_CONFIG。
        """
        try:
            producer = Producer(KafkaConfig.PRODUCER_CONFIG)
            logger.info("✅ Kafka Producer created successfully.")
            return producer
        except KafkaException as e:
            logger.critical(f"❌ Failed to create Kafka Producer: {e}")
            sys.exit(1)

    def _signal_handler(self, signum, frame) -> None:
        """
        捕捉 SIGTERM / SIGINT（Ctrl+C），執行 Graceful Shutdown。
        不立刻退出，而是設定旗標，讓主迴圈自行結束。
        """
        logger.warning(f"⛔ Signal {signum} received → initiating graceful shutdown...")
        self._running = False

    def _serialize(self, msg) -> bytes:
        """
        將 WatchTelemetry 序列化為 UTF-8 JSON bytes。
        Kafka 傳輸的 value 必須是 bytes。
        """
        return json.dumps(
            msg.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),  # 緊湊格式，減少傳輸大小
        ).encode("utf-8")

    def _produce_message(self, msg) -> None:
        """
        送出單筆訊息到 Kafka。

        關鍵設計：
          - key = device_id（str → bytes）
            → 相同 device_id 永遠進同一個 Partition
            → Consumer 可以保證單一裝置的時序
          - on_delivery = self._tracker.on_delivery
            → 非同步 callback，由 poll() 觸發
        """
        try:
            self._producer.produce(
                topic=self._topic,
                key=msg.device_id.encode("utf-8"),      # ← Partitioning Key
                value=self._serialize(msg),
                on_delivery=self._tracker.on_delivery,  # ← Delivery Callback
            )

            # poll(0)：非阻塞式觸發 callback 處理
            # 不能每筆都 poll(timeout)，那樣吞吐量會大幅下降
            self._producer.poll(0)

        except BufferError:
            # Producer 內部佇列已滿（queue.buffering.max.messages 預設 100,000）
            # 稍微等一下讓 Producer 消化
            logger.warning("⚠️  Producer queue full → waiting 100ms...")
            self._producer.poll(0.1)

        except KafkaException as e:
            logger.error(f"❌ Kafka produce error: {e}")

    # ── 公開方法 ──────────────────────────────────────────────────────────────

    def trigger_burst(self, duration_sec: int = 60) -> None:
        """切換到爆量模式（可由外部執行緒呼叫）。"""
        self._generator.trigger_burst(duration_sec=duration_sec)

    def run(self) -> None:
        """
        主迴圈：持續產生資料並送往 Kafka。

        速率控制策略：
          - 正常模式：100 msg/sec（每次送完 sleep 10ms）
          - 爆量模式：盡可能快（不 sleep，靠 linger.ms 批次優化）

        這裡選擇讓 Producer 自己控速（而非 generator.stream() 的速率控制），
        原因是爆量模式需要去掉 sleep，靠 linger.ms 達到最大吞吐量。
        """
        self._running = True
        logger.info("🚀 IoTProducer started.")

        try:
            while self._running:
                loop_start = time.perf_counter()

                # 產生一筆資料並送出
                msg = self._generator.generate_one()
                self._produce_message(msg)

                # 吞吐量報告（每秒一次）
                report = self._tracker.throughput_report()
                if report:
                    logger.info(report)

                # 速率控制
                current_mode = self._generator.current_mode
                if current_mode == SimulationMode.NORMAL:
                    # 正常模式：目標 100 msg/sec → 每筆間隔 10ms
                    elapsed = time.perf_counter() - loop_start
                    sleep_time = 0.01 - elapsed  # 10ms - 已花費時間
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                # 爆量模式：不 sleep，全速送出

        except Exception as e:
            logger.critical(f"💥 Unexpected error in producer loop: {e}", exc_info=True)

        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """
        優雅關閉：
        1. flush() 確保 Producer buffer 內的訊息全部送出
        2. 印出最終統計
        """
        logger.info("🔄 Flushing remaining messages...")
        remaining = self._producer.flush(timeout=30)  # 最多等 30 秒

        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages were NOT delivered before shutdown.")
        else:
            logger.info("✅ All messages flushed successfully.")

        logger.info(
            f"📋 Final Stats | "
            f"total_sent={self._tracker.total_sent:,} | "
            f"total_failed={self._tracker.total_failed:,}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 快速驗證（直接執行這個檔案時使用）
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logger("iot-producer")

    print("=" * 60)
    print("🧪 IoTProducer — 快速驗證")
    print("   正常模式送 200 筆，接著爆量模式送 10 秒")
    print("   按 Ctrl+C 可提前結束")
    print("=" * 60)

    # ── 模式 1：正常模式送 200 筆後自動切換爆量 ──────────────
    producer = IoTProducer()

    # 用 threading 在背景 5 秒後觸發爆量（模擬外部觸發）
    def delayed_burst():
        time.sleep(5)
        logger.warning("🎯 [Test] Triggering burst mode after 5 seconds...")
        producer.trigger_burst(duration_sec=10)

    burst_thread = threading.Thread(target=delayed_burst, daemon=True)
    burst_thread.start()

    # 主迴圈（會在 Ctrl+C 或執行完後結束）
    producer.run()
