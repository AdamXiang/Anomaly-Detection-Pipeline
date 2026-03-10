#!/bin/bash
# setup.sh：建立所有必要的目錄與設定檔，執行一次即可
set -e

echo "════════════════════════════════════════"
echo "  IoT 監控系統 - 環境初始化腳本"
echo "════════════════════════════════════════"

# 1. 建立目錄結構
echo "📁 檢查並建立目錄結構..."

# 將所有需要的目錄放進陣列中
DIRECTORIES=(
  "prometheus"
  "grafana/provisioning/datasources"
  "grafana/provisioning/dashboards"
  "grafana/dashboards"
  "postgres"
  "python"
)

# 跑迴圈逐一檢查
for dir in "${DIRECTORIES[@]}"; do
  if [ ! -d "$dir" ]; then
    mkdir -p "$dir"
    echo "  ✅ 已建立: $dir"
  else
    echo "  ⏭️ 已存在，跳過: $dir"
  fi
done

# 2. 生成 Kafka Cluster ID
echo "🔑 生成 Kafka Cluster ID..."
CLUSTER_ID=$(python3 -c "import uuid, base64; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))")
echo "   生成的 Cluster ID：$CLUSTER_ID"

# 3. 建立 .env
if [ -f .env ]; then
  # 更新已有的 .env
  sed -i '' "s/^KAFKA_CLUSTER_ID=.*/KAFKA_CLUSTER_ID=${CLUSTER_ID}/" .env
  echo "✅ .env 已更新"
else
  # 建立新的 .env
  cat > .env << EOF
POSTGRES_USER=admin
POSTGRES_PASSWORD=password
POSTGRES_DB=sports_iot
GRAFANA_ADMIN_PASSWORD=admin123
KAFKA_CLUSTER_ID=${CLUSTER_ID}
EOF
  echo "✅ .env 已建立"
fi

# 4. 加入 .gitignore
echo "🔒 更新 .gitignore..."
grep -qxF '.env' .gitignore 2>/dev/null || echo '.env' >> .gitignore

echo ""
echo "✅ 初始化完成！"
echo ""
echo "下一步："
echo "  1. docker compose up -d postgres"
echo "  2. docker compose up -d kafka-1 kafka-2 kafka-3"
echo "  3. docker compose up -d kafka-exporter node-exporter postgres-exporter"
echo "  4. docker compose up -d prometheus"
echo "  5. docker compose up -d grafana"
echo ""
echo "驗證服務："
echo "  Grafana    → http://localhost:3000  (admin / admin123)"
echo "  Prometheus → http://localhost:9090"
echo "  PostgreSQL → localhost:5432        (admin / password)"
