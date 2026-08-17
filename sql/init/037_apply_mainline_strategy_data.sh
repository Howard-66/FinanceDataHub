#!/bin/bash
set -Eeuo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -f /app/sql/migrations/037_create_mainline_strategy_data.sql
