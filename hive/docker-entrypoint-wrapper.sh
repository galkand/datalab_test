#!/usr/bin/env bash
set -euo pipefail


mkdir -p /home/hive/.beeline || true
mkdir -p /tmp/hive-local /tmp/hive-resources /tmp/hive || true
chmod 1777 /tmp 2>/dev/null || true
chmod 777 /tmp/hive-local /tmp/hive-resources /tmp/hive 2>/dev/null || true


if [ -n "${HIVE_CUSTOM_CONF_DIR:-}" ] && [ -d "${HIVE_CUSTOM_CONF_DIR}" ]; then
  echo "[hive] applying custom conf from ${HIVE_CUSTOM_CONF_DIR}"

  for f in "${HIVE_CUSTOM_CONF_DIR}"/*; do
    [ -f "$f" ] || continue
    tgt="/opt/hive/conf/$(basename "$f")"


    if [ -e "$tgt" ] && [ "$(readlink -f "$tgt")" = "$(readlink -f "$f")" ]; then
      continue
    fi

    cp -f "$f" "$tgt" || true
  done
fi


export HADOOP_CLIENT_OPTS="${SERVICE_OPTS:-} ${HADOOP_CLIENT_OPTS:-}"

if [ "${SERVICE_NAME:-}" = "metastore" ]; then
  echo "[metastore] checking schema (schematool -info)..."
  if /opt/hive/bin/schematool -dbType "${DB_DRIVER:-postgres}" -info >/tmp/schema_info.txt 2>&1; then
    echo "[metastore] schema exists -> skip init"
  else
    echo "[metastore] schema not detected -> initSchema"
    /opt/hive/bin/schematool -dbType "${DB_DRIVER:-postgres}" -initSchema
  fi

  echo "[metastore] starting metastore service..."
  exec /opt/hive/bin/hive --service metastore
fi

exec /entrypoint.sh "$@"
