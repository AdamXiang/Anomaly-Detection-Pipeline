#!/bin/bash
# ============================================
# Anomaly 1: Kafka Broker 宕機模擬
# ============================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Anomaly 1: Kafka Broker 宕機模擬      ${NC}"
echo -e "${YELLOW}========================================${NC}"

PROJECT_DIR="${HOME}/Desktop/project/Monitor"
cd "${PROJECT_DIR}"

# --- 宕機前狀態 ---
echo -e "\n${GREEN}[BEFORE] 宕機前 Broker 狀態：${NC}"
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('  Broker 數量:', d['data']['result'][0]['value'][1])"

echo -e "\n${RED}[ACTION] 停止 kafka-3 模擬宕機...${NC}"
docker compose stop kafka-3
echo -e "${RED}  kafka-3 已停止 ✓${NC}"

echo -e "\n${YELLOW}[WAIT] 等待 15 秒讓 Prometheus 偵測...${NC}"
for i in $(seq 15 -1 1); do
  echo -ne "  倒數: ${i}s \r"; sleep 1
done

echo -e "\n${RED}[DURING] 宕機中 Broker 狀態：${NC}"
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('  Broker 數量:', d['data']['result'][0]['value'][1])"

echo -e "\n${YELLOW}  Grafana: http://localhost:3000/d/iot-infra-002${NC}"
echo -e "${YELLOW}  → Broker 數量應顯示 2（紅色告警）${NC}"
read -p "  截圖完成？按 Enter 恢復... "

echo -e "\n${GREEN}[RECOVER] 重啟 kafka-3...${NC}"
docker compose start kafka-3

echo -e "\n${YELLOW}[WAIT] 等待 30 秒讓 Broker 重新加入...${NC}"
for i in $(seq 30 -1 1); do
  echo -ne "  倒數: ${i}s \r"; sleep 1
done

echo -e "\n${GREEN}[AFTER] 恢復後 Broker 狀態：${NC}"
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('  Broker 數量:', d['data']['result'][0]['value'][1])"

echo -e "\n${GREEN}  Anomaly 1 完成！3→2（告警）→3（恢復）✅${NC}"
EOF

chmod +x ~/Desktop/project/Monitor/python/anomaly/anomaly_1_broker_failure.sh
echo "✅ anomaly_1 已修正"
