```bash
# 1) Nuke any old test container (safe if it doesn't exist)
docker rm -f mongo-test 2>/dev/null || true

# 2) Start a clean instance with NO host volume (rules out permissions issues)
docker run -d --name mongo-test \
  -p 27018:27017 \
  mongo:6.0 --bind_ip_all

# 3) Watch startup logs (look for "Waiting for connections" and "port: 27017")
docker logs -f mongo-test | sed -n '1,120p'

# 4) From host, verify the port and connect
nc -vz 127.0.0.1 27018
mongosh "mongodb://127.0.0.1:27018"
```

use TradeStoreDB

db.trades.find().pretty()

 db.trades.count()






If this works → your earlier failure was almost certainly due to **volume path/permissions** or an **invalid config**. Keep using `27018` or switch to `27017` after freeing it.

# If you need a host-mounted data directory

When you mount a host folder, it must be writable by Mongo’s user in the container.

```bash
# Example: create a data dir and (Linux) give it uid 999 (mongodb user)
mkdir -p ~/mongo-data
# Linux only:
sudo chown -R 999:999 ~/mongo-data

docker rm -f mongo 2>/dev/null || true
docker run -d --name mongo \
  -p 27017:27017 \
  -v ~/mongo-data:/data/db \
  mongo:6.0 --bind_ip_all

docker logs -f mongo | sed -n '1,120p'
nc -vz 127.0.0.1 27017
mongosh "mongodb://127.0.0.1:27017"
```
