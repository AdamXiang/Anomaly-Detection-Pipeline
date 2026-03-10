#!/bin/bash
# ============================================
# Anomaly 3 執行器（Shell Wrapper）
# ============================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_DIR="${HOME}/Desktop/project/Monitor"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"
SCRIPT="${PROJECT_DIR}/python/anomaly/anomaly_3_db_pressure.py"

echo -e "${YELLOW}[CHECK] 確認 venv 路徑...${NC}"
if [ ! -f "${VENV_ACTIVATE}" ]; then
  echo -e "${RED}[ERROR] 找不到 .venv：${VENV_ACTIVATE}${NC}"
  exit 1
fi
echo -e "${GREEN}  venv ✓${NC}"

echo -e "${YELLOW}[CHECK] 確認腳本存在...${NC}"
if [ ! -f "${SCRIPT}" ]; then
  echo -e "${RED}[ERROR] 找不到腳本：${SCRIPT}${NC}"
  exit 1
fi
echo -e "${GREEN}  腳本 ✓${NC}"

echo -e "${YELLOW}[ACTION] 啟動 Anomaly 3...${NC}\n"

# 啟動 venv 並用 python3 執行
source "${VENV_ACTIVATE}"
python3 "${SCRIPT}"