# trade-store

DockerKafka 

docker pull redpandadata/redpanda:latest

docker run -it --rm \
  --name redpanda2 \
  -p 9093:9092 -p 9645:9644 \
  redpandadata/redpanda:latest redpanda start \
    --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
    --check=false --node-id=1 \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr PLAINTEXT://127.0.0.1:9093



Install python packages 
uvicorn



AIOKafka
pip install aiokafka
# Optional: export settings
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export KAFKA_TOPIC=aiokafka_healthcheck
python kafka_aiokafka_healthcheck.py && echo "OK" || echo "FAILED"





MongoDB

python3 -m pip install pymongo

docker run -d --name mongodb \
  -p 27017:27017 \
  -v mongodata:/data/db \
  mongo:7


mongo client - mongosh "mongodb://127.0.0.1:27017" -- to access view data
use TradeStoreDB
db.trades.find().pretty()
