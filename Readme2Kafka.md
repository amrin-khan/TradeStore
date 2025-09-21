You can run **Redpanda** with no host dependencies (no mounted volumes, no bind-paths) by just starting it in **ephemeral mode**. All logs and topic data stay inside the container, and will be deleted when you remove the container.
Here’s a simple `docker run` command:

```bash
docker run -d --name redpanda \
  -p 9092:9092 -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start \
    --overprovisioned \
    --smp 1 \
    --memory 1G \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr PLAINTEXT://localhost:9092
```

---

### 🔹 What this does

* `-d` → runs detached (in background).
* `-p 9092:9092` → Kafka API available on host port 9092.
* `-p 9644:9644` → Redpanda Admin API.
* **No `-v` volumes** → data lives only inside the container.
* `--overprovisioned` + `--smp 1` + `--memory 1G` → good for local development with minimal resources.
* `--advertise-kafka-addr PLAINTEXT://localhost:9092` → makes it easy for clients on your host to connect at `localhost:9092`.

---

### 🔹 Verify it’s running

```bash
docker logs -f redpanda | head -50
```

Look for:
`INFO  Started Kafka API server listening at ...:9092`

---

### 🔹 Create a topic

```bash
docker exec -it redpanda rpk topic create trades --brokers=localhost:9092
docker exec -it redpanda rpk topic list --brokers=localhost:9092
```

---

### 🔹 Produce / Consume test messages

Produce:

```bash
docker exec -it redpanda rpk topic produce trades --brokers=localhost:9092
```

(type a few lines, then press **Ctrl+D**)

Consume:

```bash
docker exec -it redpanda rpk topic consume trades --brokers=localhost:9092
```

---

✅ With this setup, you have a fully working Kafka-compatible broker at `localhost:9092`, no host filesystem dependencies, and no risk of permission issues.

Do you want me to also prepare a **`docker-compose.yml`** version with the same “no-host-volumes” setup (just ephemeral containers)?
