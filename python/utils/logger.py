# python/utils/logger.py

import sys
from loguru import logger

def setup_logger(name: str = "iot-pipeline", level: str = "INFO") -> None:
    """
    統一設定 loguru logger
    輸出格式：時間 | level | 模組名稱 | 訊息
    """
    logger.remove()  # 移除預設 handler

    # Console 輸出
    logger.add(
        sys.stdout,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan> | "
            "<white>{message}</white>"
        ),
        colorize=True,
    )

    # File 輸出（自動 rotation）
    logger.add(
        f"logs/{name}.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}",
        rotation="50 MB",   # 超過 50MB 就換新檔案
        retention="7 days", # 只保留 7 天的 log
        compression="zip",  # 舊 log 壓縮保存
    )

    logger.info(f"Logger initialized: name={name}, level={level}")
