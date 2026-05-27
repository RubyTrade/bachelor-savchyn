#!/usr/bin/env bash
set -x
set -euo pipefail

KAFKA_VERSION="${kafka_version}"
KAFKA_DIST="kafka_$${KAFKA_VERSION}"
KAFKA_TGZ="kafka_2.13-$${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/$${KAFKA_VERSION}/$${KAFKA_TGZ}"

apt-get update
DEBIAN_FRONTEND=noninteractive 

apt-get install -y openjdk-17-jdk wget tar 

INSTANCE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

useradd --no-create-home --shell /bin/false kafka || true
mkdir -p /opt/kafka
chown -R kafka:kafka /opt/kafka
mkdir -p /var/lib/kraft-logs
chown -R kafka:kafka /var/lib/kraft-logs

# download Kafka
cd /opt
if [ ! -f "/opt/$${KAFKA_TGZ}" ]; then
  wget -q "$${KAFKA_URL}" -O "$${KAFKA_TGZ}"
fi
tar -xzf "$${KAFKA_TGZ}" --strip-components=1 -C /opt/kafka
chown -R kafka:kafka /opt/kafka

# minimal KRaft config (single-node)
cat > /opt/kafka/config/kraft/server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@$${INSTANCE_IP}:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://$${INSTANCE_IP}:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/lib/kraft-logs
auto.create.topics.enable=true
# recommended tuning for single-node experiment:
num.network.threads=3
num.io.threads=8
queued.max.requests=500

# REQUIRED for single-node Kafka
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
chown kafka:kafka /opt/kafka/config/kraft/server.properties

# create systemd unit
cat > /etc/systemd/system/kafka.service <<'EOF'
[Unit]
Description=Apache Kafka (KRaft) broker
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

if [ ! -f /var/lib/kraft-logs/meta.properties ]; then
  CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
  /opt/kafka/bin/kafka-storage.sh format \
    -t "$${CLUSTER_ID}" \
    -c /opt/kafka/config/kraft/server.properties
fi

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

for i in $(seq 1 20); do
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 3
done

# TODO: create a proper topic
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic trades \
  --partitions 1 \
  --replication-factor 1
