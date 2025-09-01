kafka-topics --alter \
  --topic network-logs \
  --partitions 3 \
  --bootstrap-server localhost:9092
