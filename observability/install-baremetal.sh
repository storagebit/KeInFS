#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-2.0-or-later
# Install Prometheus + Grafana on a bare Ubuntu/Debian observability box and
# wire in the KeInFS scrape config + dashboards. Idempotent-ish; run with sudo.
#
#   sudo ./install-baremetal.sh
#
# Then: Grafana http://<box>:3000 (admin/admin), Prometheus http://<box>:9090.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== installing Prometheus (apt) =="
apt-get update -y
apt-get install -y prometheus prometheus-node-exporter || apt-get install -y prometheus

echo "== installing Grafana (official apt repo) =="
apt-get install -y apt-transport-https software-properties-common wget gnupg
mkdir -p /etc/apt/keyrings
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor > /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
  > /etc/apt/sources.list.d/grafana.list
apt-get update -y
apt-get install -y grafana

echo "== installing KeInFS scrape config =="
install -m0644 "$HERE/prometheus/prometheus.yml" /etc/prometheus/prometheus.yml

echo "== installing Grafana provisioning + dashboards =="
install -d /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards
install -m0644 "$HERE/grafana/provisioning/datasources/prometheus.yml" /etc/grafana/provisioning/datasources/keinfs.yml
# point the dashboard provider at the installed dashboards dir
install -d /var/lib/grafana/dashboards
install -m0644 "$HERE"/grafana/dashboards/*.json /var/lib/grafana/dashboards/
cat > /etc/grafana/provisioning/dashboards/keinfs.yml <<'YAML'
apiVersion: 1
providers:
  - name: KeInFS
    orgId: 1
    folder: KeInFS
    type: file
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
YAML
chown -R grafana:grafana /var/lib/grafana/dashboards 2>/dev/null || true

echo "== enable + start services =="
systemctl enable --now prometheus
systemctl enable --now grafana-server
systemctl restart prometheus grafana-server

echo "== done =="
echo "Prometheus: http://$(hostname -I | awk '{print $1}'):9090"
echo "Grafana:    http://$(hostname -I | awk '{print $1}'):3000  (admin/admin)"
