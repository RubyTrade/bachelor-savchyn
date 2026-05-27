#!/usr/bin/env bash
set -x
set -euo pipefail
set -e

apt-get update -y

apt-get install -y \
  apt-transport-https \
  software-properties-common \
  wget

mkdir -p /etc/apt/keyrings/

wget -q -O - https://apt.grafana.com/gpg.key | \
  gpg --dearmor | \
  tee /etc/apt/keyrings/grafana.gpg > /dev/null

echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | \
  tee -a /etc/apt/sources.list.d/grafana.list

apt-get update -y

apt-get install -y grafana

/usr/sbin/grafana cli \
  --homepath /usr/share/grafana \
  plugins install grafana-athena-datasource

mkdir -p /etc/grafana/provisioning/datasources

cat > /etc/grafana/provisioning/datasources/athena.yaml <<EOF
apiVersion: 1

datasources:
  - name: Athena
    type: grafana-athena-datasource
    access: proxy
    isDefault: true

    jsonData:
      authType: default
      defaultRegion: us-east-1
      catalog: AwsDataCatalog
      database: gold
      workgroup: grafana-workgroup
EOF

systemctl daemon-reload
systemctl enable grafana-server
systemctl restart grafana-server
