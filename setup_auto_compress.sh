#!/bin/bash
# Setup automatic compression cron job for Polyrecord

CRON_JOB="0 3 * * * cd ~/polyrecord && /home/ubuntu/polyrecord/.venv/bin/python3 compress.py --days 1 >> /home/ubuntu/polyrecord/data/logs/compress.log 2>&1"

# Create logs directory if it doesn't exist
mkdir -p ~/polyrecord/data/logs

# Add cron job (remove existing polyrecord compress jobs first, then add new)
crontab -l 2>/dev/null | grep -v "compress.py" | grep -v "^#" > /tmp/current_cron
echo "$CRON_JOB" >> /tmp/current_cron
crontab /tmp/current_cron
rm /tmp/current_cron

echo "Cron job installed:"
crontab -l | grep compress

echo ""
echo "Compression will run daily at 3:00 AM UTC"
echo "Logs will be written to: ~/polyrecord/data/logs/compress.log"
