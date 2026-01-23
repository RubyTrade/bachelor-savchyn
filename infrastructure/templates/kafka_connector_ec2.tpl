#!/usr/bin/env bash
set -x
set -euo pipefail

KAFKA_VERSION="${kafka_version}"
KAFKA_DIST="kafka_$${KAFKA_VERSION}"
KAFKA_TGZ="kafka_2.13-$${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/$${KAFKA_VERSION}/$${KAFKA_TGZ}"
BOOTSTRAP_SERVER="${kafka_bootstrap}:9092"
CONNECTOR_NAME="s3-sink-trades"
S3_BUCKET_NAME="${s3_bucket}"
AWS_REGION="${aws_region}"

INSTANCE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

# sleep for 5 minutes, to ensure that kafka broker instance is set up
sleep 300

apt-get update
DEBIAN_FRONTEND=noninteractive 

apt-get install -y openjdk-17-jdk wget tar curl jq

useradd --no-create-home --shell /bin/false kafka || true

mkdir -p /opt/kafka /opt/kafka/plugins

# download Kafka
cd /opt
if [ ! -f "/opt/$${KAFKA_TGZ}" ]; then
  wget -q "$${KAFKA_URL}" -O "$${KAFKA_TGZ}"
fi
tar -xzf "$${KAFKA_TGZ}" --strip-components=1 -C /opt/kafka

chown -R kafka:kafka /opt/kafka

# Confluent Hub
pushd "/opt"
wget -q https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz
tar -xzf confluent-hub-client-latest.tar.gz
popd

export PATH=$PATH:/opt/bin

/opt/bin/confluent-hub install \
  confluentinc/kafka-connect-s3:10.5.0 \
  --component-dir /opt/kafka/plugins \
  --worker-configs /opt/kafka/config/connect-distributed.properties \
  --no-prompt

cat > /opt/kafka/config/connect-distributed.properties <<EOF
bootstrap.servers=$${BOOTSTRAP_SERVER}
group.id=connect-cluster

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

consumer.group.initial.rebalance.delay.ms=0
request.timeout.ms=60000
session.timeout.ms=30000
heartbeat.interval.ms=10000

offset.storage.topic=connect-offsets
config.storage.topic=connect-configs
status.storage.topic=connect-status

offset.storage.replication.factor=1
config.storage.replication.factor=1
status.storage.replication.factor=1

rest.advertised.host.name=$${INSTANCE_IP}
rest.advertised.port=8083

plugin.path=/opt/kafka/plugins
EOF

touch /var/log/kafka-connect.log /var/log/kafka-connect.err
chown kafka:kafka /var/log/kafka-connect.log /var/log/kafka-connect.err

cat > /etc/systemd/system/kafka-connect.service <<EOF
[Unit]
Description=Kafka Connect
After=network.target

[Service]
User=kafka
ExecStart=/opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties 
Restart=on-failure
StandardOutput=append:/var/log/kafka-connect.log
StandardError=append:/var/log/kafka-connect.err

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kafka-connect
systemctl start kafka-connect

echo "Waiting for Kafka Connect REST API..."
for i in $(seq 1 60); do
  if curl -s http://localhost:8083/connectors >/dev/null; then
    echo "Kafka Connect is up"
    break
  fi
  sleep 5
done

# sleep for 2 minutes, to ensure that kafka connector worker is up
echo "Waiting for Kafka Connect to become READY..."
sleep 120

if curl -s http://localhost:8083/connectors | jq -e ".[] | select(. == \"$${CONNECTOR_NAME}\")" >/dev/null; then
  echo "Connector already exists"
else
  echo "Creating S3 Sink Connector"

  curl -s -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d '{
      "name": "'"$${CONNECTOR_NAME}"'",
      "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "trades",
        "s3.bucket.name": "'"$${S3_BUCKET_NAME}"'",
        "s3.region": "'"$${AWS_REGION}"'",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "flush.size": "2"
      }
    }'
fi
