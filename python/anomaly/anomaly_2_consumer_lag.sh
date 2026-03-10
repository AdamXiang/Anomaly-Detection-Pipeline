#!/bin/bash
# ============================================
# Anomaly 2: Consumer Lag 爆增模擬
# ============================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Anomaly 2: Consumer Lag 爆增模擬      ${NC}"
echo -e "${YELLOW}========================================${NC}"

# ── 路徑定義（已修正）───────────────────────
PROJECT_DIR="${HOME}/Desktop/project/Monitor"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"
CONSUMER_SCRIPT="${PROJECT_DIR}/python/pipeline/consumer.py"

echo -e "\n  專案根目錄 : ${PROJECT_DIR}"
echo -e "  venv 路徑  : ${VENV_ACTIVATE}"
echo -e "  Consumer   : ${CONSUMER_SCRIPT}"

# ── 確認 venv 存在 ───────────────────────────
if [ ! -f "${VENV_ACTIVATE}" ]; then
  echo -e "\n${RED}[ERROR] 找不到 venv！路徑確認失敗：${NC}"
  echo -e "${RED}  ${VENV_ACTIVATE}${NC}"
  exit 1
fi
echo -e "${GREEN}\n  venv 路徑確認 ✓${NC}"

# ── 停止 Consumer 模擬崩潰 ──────────────────
echo -e "\n${RED}[ACTION] 停止 Consumer，模擬崩潰...${NC}"
pkill -f "consumer.py" 2>/dev/null && \
  echo -e "${RED}  Consumer 已停止 ✓${NC}" || \
  echo -e "${YELLOW}  Consumer 本來就沒在跑（直接進入觀察）${NC}"

sleep 2

# ── 觀察 Lag 累積 ────────────────────────────
echo -e "\n${YELLOW}[OBSERVE] 觀察 Lag 累積（30 秒）...${NC}"
echo -e "${YELLOW}  Grafana: http://localhost:3000/d/iot-infra-002${NC}"
echo -e "${YELLOW}  觀察 Consumer Group Lag 趨勢圖上升 📈${NC}\n"

for i in $(seq 1 6); do
  LAG=$(curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag)" | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d['data']['result']
print(int(float(r[0]['value'][1])) if r else 'N/A')
" 2>/dev/null)
  echo "  [$(date '+%H:%M:%S')] Consumer Lag = ${LAG} messages 🔴"
  sleep 5
done

read -p $'\n  截圖完成？按 Enter 啟動 Consumer 追回 Lag... '

# ── 啟動 Consumer 追回 ───────────────────────
echo -e "\n${GREEN}[RECOVER] 啟動 Consumer 追回 Lag...${NC}"

source "${VENV_ACTIVATE}" && \
  nohup python3 "${CONSUMER_SCRIPT}" > /tmp/consumer_recovery.log 2>&1 &

CONSUMER_PID=$!
echo -e "${GREEN}  Consumer 已啟動 (PID: ${CONSUMER_PID}) ✓${NC}"
echo -e "${GREEN}  Log: tail -f /tmp/consumer_recovery.log${NC}"

# ── 觀察 Lag 下降 ────────────────────────────
echo -e "\n${YELLOW}[OBSERVE] 觀察 Lag 下降（50 秒）...${NC}\n"

for i in $(seq 1 10); do
  LAG=$(curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag)" | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d['data']['result']
print(int(float(r[0]['value'][1])) if r else 'N/A')
" 2>/dev/null)
  echo "  [$(date '+%H:%M:%S')] Consumer Lag = ${LAG} messages 📉"
  sleep 5
done

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Anomaly 2 完成！                       ${NC}"
echo -e "${GREEN}  Consumer 崩潰 → Lag 爆增 → 重啟追回 ✅ ${NC}"
echo -e "${GREEN}========================================${NC}"