#!/bin/bash
# Warden — guards Scribe (ingest_deepseek.py). Restart on death; respect idle marker.
WORK_DIR="${MIRROR_INGEST_DIR:-$HOME/Mirror-Food/ingest}"
LOG="$WORK_DIR/warden.log"
mkdir -p "$WORK_DIR" "$HOME/logs"
while true; do
    if ! pgrep -f "ingest_deepseek.py" > /dev/null; then
        if [ -f "$WORK_DIR/.ingest-idle" ]; then
            sleep 1800; continue
        fi
        echo "$(date): Scribe not running. Restarting..." >> "$LOG"
        if pgrep ollama > /dev/null; then
            nohup caffeinate -i python3 "$WORK_DIR/ingest_deepseek.py" >> "$WORK_DIR/full-run.log" 2>&1 &
            echo "$(date): Restarted PID $!" >> "$LOG"
        else
            echo "$(date): Ollama down. Skipping restart." >> "$LOG"
        fi
    fi
    sleep 300
done
