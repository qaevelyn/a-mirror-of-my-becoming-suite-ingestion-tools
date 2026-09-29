#!/bin/bash
# Watchdog: restarts ingest_msgvault if it dies.
# Sleep 300s. Skip if idle marker present.

while true; do
    if ! pgrep -f "ingest_msgvault.py" > /dev/null; then
        if [ -f ~/Mirror-Food/ingest/.ingest-idle ]; then
            # Script exited because there was nothing to do. Wait longer.
            sleep 1800
            continue
        fi
        echo "$(date): Ingest not running. Restarting..." >> ~/logs/ingest-watchdog.log
        if pgrep ollama > /dev/null; then
            nohup caffeinate -i python3 ~/Mirror-Food/ingest/ingest_msgvault.py >> ~/Mirror-Food/ingest/full-run.log 2>&1 &
            echo "$(date): Restarted PID $!" >> ~/logs/ingest-watchdog.log
        else
            echo "$(date): Ollama down. Skipping restart." >> ~/logs/ingest-watchdog.log
        fi
    fi
    sleep 300
done
