# trade-store

DockerKafka 

docker pull redpandadata/redpanda:latest

docker run -d --name redpanda \
  -p 9092:9092 -p 9644:9644 \
  redpandadata/redpanda:latest \
  redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
  --node-id 0 --check=false \
  --kafka-addr 0.0.0.0:9092 \
  --advertise-kafka-addr 127.0.0.1:9092




Install python packages 
uvicorn





MongoDB

python3 -m pip install pymongo

docker run -d --name mongodb \
  -p 27017:27017 \
  -v mongodata:/data/db \
  mongo:7


mongo client - mongosh "mongodb://127.0.0.1:27017" -- to access view data
use TradeStoreDB
db.trades.find().pretty()
