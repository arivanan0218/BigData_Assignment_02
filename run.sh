#!/usr/bin/env bash
# usage: ./run.sh {setup|up|down|producer|consumer|topics}
set -e
cd "$(dirname "$0")"
case "$1" in
  setup)    python3 -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements.txt ;;
  up)       docker compose up -d ;;
  down)     docker compose down -v ;;
  producer) exec .venv/bin/python -m app.producer "${@:2}" ;;
  consumer) exec .venv/bin/python -m app.consumer ;;
  topics)   docker exec redpanda-4414 rpk topic list ;;
  *)        echo "usage: $0 {setup|up|down|producer|consumer|topics}"; exit 1 ;;
esac
