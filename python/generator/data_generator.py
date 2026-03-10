# python/generator/data_generator.py

import random
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Generator

from faker import Faker
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# Enums & Constants
# ─────────────────────────────────────────────────────────────────────────────

class SimulationMode(Enum):
    NORMAL = "normal"
    BURST  = "burst"


# 心率範圍（依模式區分）
HEART_RATE_NORMAL = (60, 100)   # 靜息 / 日常活動
HEART_RATE_BURST  = (130, 185)  # 高強度運動（馬拉松）

# 血氧範圍（SpO2 %）
BLOOD_OXYGEN_NORMAL = (96.0, 100.0)
BLOOD_OXYGEN_BURST  = (92.0, 98.0)  # 高強度運動時血氧微降

# 台北馬拉松起跑點附近的 GPS 範圍（僅供模擬）
GPS_LAT_RANGE  = (25.020, 25.075)
GPS_LON_RANGE  = (121.520, 121.580)


# ─────────────────────────────────────────────────────────────────────────────
# Data Model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WatchTelemetry:
    """
    單筆手錶遙測資料。
    使用 dataclass 的好處：
      - 自動生成 __init__, __repr__
      - asdict() 可以直接轉成 dict → JSON
    """
    device_id:     str
    user_id:       str
    event_time:    str          # ISO 8601，手錶端時間（非伺服器時間）
    heart_rate:    int          # bpm
    blood_oxygen:  float        # SpO2 %，保留一位小數
    steps:         int          # 累積步數（session 內累積，非全天）
    battery_level: int          # 電量 %
    latitude:      float        # GPS 緯度，保留 6 位小數
    longitude:     float        # GPS 經度，保留 6 位小數
    mode:          str          # "normal" / "burst"（方便 Grafana 過濾）

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# Device State（追蹤每個裝置的累積狀態）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DeviceState:
    """
    追蹤單一裝置的可變狀態。
    不能每次都隨機，步數必須累積遞增才符合現實。
    """
    device_id:    str
    user_id:      str
    steps:        int   = 0     # 累積步數（session 開始後只增不減）
    battery:      int   = field(default_factory=lambda: random.randint(60, 100))

    # GPS 起始點（每個裝置有獨立起始位置，避免全部擠在同一點）
    base_lat: float = field(default_factory=lambda: random.uniform(*GPS_LAT_RANGE))
    base_lon: float = field(default_factory=lambda: random.uniform(*GPS_LON_RANGE))

    def step_increment(self, mode: SimulationMode) -> int:
        """
        每次呼叫增加步數。
        正常模式：每次增加 0-3 步（慢走）
        爆量模式：每次增加 3-8 步（跑步）
        """
        if mode == SimulationMode.NORMAL:
            increment = random.randint(0, 3)
        else:
            increment = random.randint(3, 8)

        self.steps += increment
        return self.steps

    def drain_battery(self) -> int:
        """
        電量緩慢下降（每 200 次資料點才降 1%）
        不需要每一筆都降，太快就不真實了。
        """
        if random.random() < 0.005:   # 0.5% 機率降電量
            self.battery = max(0, self.battery - 1)
        return self.battery

    def gps_drift(self) -> tuple[float, float]:
        """
        GPS 位置微幅漂移（模擬跑步移動）
        每次在起始點附近加一個微小偏移。
        """
        lat = self.base_lat + random.uniform(-0.001, 0.001)
        lon = self.base_lon + random.uniform(-0.001, 0.001)

        # 更新 base（持續往某方向移動，模擬跑步路線）
        self.base_lat += random.uniform(-0.0002, 0.0002)
        self.base_lon += random.uniform(-0.0002, 0.0002)

        return round(lat, 6), round(lon, 6)


# ─────────────────────────────────────────────────────────────────────────────
# IoT Data Generator（核心類別）
# ─────────────────────────────────────────────────────────────────────────────

class IoTDataGenerator:
    """
    模擬多台 IoT 智慧手錶的資料產生器。

    使用方式：
        generator = IoTDataGenerator(num_devices=50)

        # 正常模式（阻塞式，持續產生）
        for msg in generator.stream(mode=SimulationMode.NORMAL):
            producer.send(msg)

        # 爆量模式（60 秒後自動切回正常模式）
        generator.trigger_burst(duration_sec=60)
    """

    def __init__(self, num_devices: int = 50):
        self.fake = Faker()
        self.num_devices = num_devices
        self._mode = SimulationMode.NORMAL
        self._burst_end_time: float = 0.0   # 爆量結束的 epoch time

        # 初始化所有裝置狀態
        self._devices: list[DeviceState] = self._init_devices(num_devices)

        logger.info(
            f"IoTDataGenerator initialized | "
            f"num_devices={num_devices} | "
            f"mode={self._mode.value}"
        )

    # ── 私有方法 ──────────────────────────────────────────────────────────────

    def _init_devices(self, n: int) -> list[DeviceState]:
        """建立 n 台裝置，每台有唯一 device_id / user_id。"""
        devices = []
        for i in range(n):
            devices.append(DeviceState(
                device_id=f"watch-{str(uuid.uuid4())[:8]}",
                user_id=f"user-{str(uuid.uuid4())[:8]}",
            ))
        logger.debug(f"Initialized {n} virtual devices.")
        return devices

    def _current_mode(self) -> SimulationMode:
        """
        檢查是否還在爆量時間窗內。
        如果爆量時間到了，自動切回正常模式。
        """
        if self._mode == SimulationMode.BURST:
            if time.time() > self._burst_end_time:
                logger.info("⏱️  Burst duration ended → switching back to NORMAL mode.")
                self._mode = SimulationMode.NORMAL
        return self._mode

    def _generate_heart_rate(self, mode: SimulationMode) -> int:
        """
        用正態分佈生成心率（比 randint 更真實）。
        正常模式：均值 78，標準差 8
        爆量模式：均值 158，標準差 12
        """
        if mode == SimulationMode.NORMAL:
            mu, sigma = 78, 8
            low, high = HEART_RATE_NORMAL
        else:
            mu, sigma = 158, 12
            low, high = HEART_RATE_BURST

        hr = int(random.gauss(mu, sigma))
        return max(low, min(high, hr))  # clamp 到合理範圍

    def _generate_blood_oxygen(self, mode: SimulationMode) -> float:
        """
        血氧值（SpO2）遵循正態分佈，保留一位小數。
        正常模式：均值 98.5，標準差 0.8
        爆量模式：均值 95.0，標準差 1.2（運動時略降）
        """
        if mode == SimulationMode.NORMAL:
            mu, sigma = 98.5, 0.8
            low, high = BLOOD_OXYGEN_NORMAL
        else:
            mu, sigma = 95.0, 1.2
            low, high = BLOOD_OXYGEN_BURST

        spo2 = round(random.gauss(mu, sigma), 1)
        return max(low, min(high, spo2))

    def _build_message(self, device: DeviceState) -> WatchTelemetry:
        """
        根據當前模式，為指定裝置產生一筆遙測資料。
        """
        mode = self._current_mode()
        lat, lon = device.gps_drift()

        return WatchTelemetry(
            device_id=device.device_id,
            user_id=device.user_id,
            event_time=datetime.now(timezone.utc).isoformat(),
            heart_rate=self._generate_heart_rate(mode),
            blood_oxygen=self._generate_blood_oxygen(mode),
            steps=device.step_increment(mode),
            battery_level=device.drain_battery(),
            latitude=lat,
            longitude=lon,
            mode=mode.value,
        )

    # ── 公開方法 ──────────────────────────────────────────────────────────────

    def trigger_burst(self, duration_sec: int = 60) -> None:
        """
        觸發爆量模式。
        持續 duration_sec 秒後自動恢復正常模式。
        """
        self._mode = SimulationMode.BURST
        self._burst_end_time = time.time() + duration_sec
        logger.warning(
            f"🚨 BURST MODE ACTIVATED | "
            f"duration={duration_sec}s | "
            f"target_rate=10,000 msg/sec"
        )

    def switch_to_normal(self) -> None:
        """立即切換回正常模式（不等 duration 到期）。"""
        self._mode = SimulationMode.NORMAL
        logger.info("✅ Switched to NORMAL mode.")

    @property
    def current_mode(self) -> SimulationMode:
        return self._current_mode()

    def generate_one(self) -> WatchTelemetry:
        """
        隨機挑選一台裝置，產生一筆資料。
        用於 Producer 的主迴圈（Producer 自己控制頻率）。
        """
        device = random.choice(self._devices)
        return self._build_message(device)

    def stream(
        self,
        rate_per_sec: int = 100,
        max_messages: int | None = None,
    ) -> Generator[WatchTelemetry, None, None]:
        """
        持續產生資料的 Generator（含速率控制）。

        Args:
            rate_per_sec:  每秒產生幾筆資料（預設 100）
            max_messages:  最多產生幾筆（None = 無限）

        Yields:
            WatchTelemetry: 一筆遙測資料
        """
        interval = 1.0 / rate_per_sec  # 每筆之間的間隔秒數
        count = 0

        logger.info(f"▶️  Stream started | rate={rate_per_sec} msg/sec")

        try:
            while True:
                start = time.perf_counter()

                msg = self.generate_one()
                yield msg
                count += 1

                # 進度 log（每 1000 筆一次）
                if count % 1000 == 0:
                    logger.info(
                        f"📊 Generated {count:,} messages | "
                        f"mode={self.current_mode.value}"
                    )

                # 達到上限就停止
                if max_messages and count >= max_messages:
                    logger.info(f"✅ Stream finished | total={count:,} messages")
                    break

                # 速率控制：睡眠剩餘時間
                elapsed = time.perf_counter() - start
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info(f"⛔ Stream interrupted by user | total={count:,} messages")


# ─────────────────────────────────────────────────────────────────────────────
# 快速驗證（直接執行這個檔案時使用）
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    print("=" * 60)
    print("🧪 IoTDataGenerator — 快速驗證")
    print("=" * 60)

    gen = IoTDataGenerator(num_devices=5)

    # ── 測試 1：正常模式，產生 3 筆 ──────────────────────────
    print("\n📋 [正常模式] 產生 3 筆資料：")
    for i, msg in enumerate(gen.stream(rate_per_sec=10, max_messages=3)):
        print(json.dumps(msg.to_dict(), indent=2, ensure_ascii=False))
        if i >= 2:
            break

    # ── 測試 2：觸發爆量模式，產生 3 筆 ──────────────────────
    print("\n🚨 [爆量模式] 觸發後產生 3 筆資料：")
    gen.trigger_burst(duration_sec=10)
    for i, msg in enumerate(gen.stream(rate_per_sec=10, max_messages=3)):
        print(json.dumps(msg.to_dict(), indent=2, ensure_ascii=False))
        if i >= 2:
            break

    print("\n✅ 驗證完成！")
