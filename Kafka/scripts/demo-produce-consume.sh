#!/usr/bin/env bash
# Kich ban demo tron goi: reset topic sach -> gui 200 su kien clickstream
# (key=session_id) -> doc lai bang 1 consumer -> chia 2 consumer cung group
# de minh hoa Kafka tu chia partition. Dung Python + kafka-python tren HOST
# (khong phai trong container) - can: pip install kafka-python
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_BIN="${PYTHON_BIN:-python}"

echo "################################################################"
echo "# BUOC 1: Reset topic ve trang thai sach"
echo "################################################################"
bash scripts/reset-topics.sh

echo ""
echo "################################################################"
echo "# BUOC 2: Producer gui 200 su kien clickstream (key=session_id)"
echo "################################################################"
"$PYTHON_BIN" producer.py --key-strategy session_id

echo ""
echo "################################################################"
echo "# BUOC 3: 1 consumer doc lai toan bo (tu dau)"
echo "################################################################"
"$PYTHON_BIN" consumer.py --group demo-solo --consumer-name SOLO \
  --from-beginning --max-messages 200 --duration 30

echo ""
echo "################################################################"
echo "# BUOC 4: 2 consumer CUNG group - Kafka tu chia partition"
echo "# (chay song song, moi consumer nhan mot phan partition)"
echo "################################################################"
"$PYTHON_BIN" consumer.py --group demo-shared --consumer-name C1 \
  --from-beginning --duration 20 &
PID1=$!
"$PYTHON_BIN" consumer.py --group demo-shared --consumer-name C2 \
  --from-beginning --duration 20 &
PID2=$!
wait "$PID1" "$PID2"

echo ""
echo "Demo hoan tat. So sanh 'partition assignment' cua C1/C2 o tren de thay"
echo "Kafka tu dong chia deu 4 partition cho 2 consumer (khong can cau hinh thu cong)."
