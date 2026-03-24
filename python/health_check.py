#!/usr/bin/env python3
"""
PostgreSQL Health Check & Alerting Script
Monitors connections, replication lag, and long-running queries
"""

import psycopg2
import smtplib
import os
from email.mime.text import MIMEText
from datetime import datetime

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "myapp"),
    "user":     os.getenv("DB_USER", "monitor_user"),
    "password": os.getenv("DB_PASSWORD"),
}

ALERT_EMAIL    = os.getenv("ALERT_EMAIL", "dba@company.com")
MAX_CONN_PCT   = 80          # alert if connections > 80%
MAX_LAG_BYTES  = 50_000_000  # alert if replication lag > 50 MB
MAX_QUERY_SECS = 300         # alert if query running > 5 minutes

def check_connections(cur) -> list[str]:
    cur.execute("""
        SELECT count(*) AS active,
               (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS max_conn
        FROM pg_stat_activity WHERE state = 'active'
    """)
    row = cur.fetchone()
    pct = (row[0] / row[1]) * 100
    if pct > MAX_CONN_PCT:
        return [f"⚠️ High connections: {row[0]}/{row[1]} ({pct:.1f}%)"]
    return []

def check_long_queries(cur) -> list[str]:
    cur.execute("""
        SELECT pid, usename, now() - query_start AS duration, query
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query_start IS NOT NULL
          AND EXTRACT(EPOCH FROM (now() - query_start)) > %s
        ORDER BY duration DESC
    """, (MAX_QUERY_SECS,))
    alerts = []
    for row in cur.fetchall():
        alerts.append(f"⚠️ Long query (PID {row[0]}, {row[1]}): {str(row[2])} — {row[3][:80]}")
    return alerts

def send_alert(alerts: list[str]):
    body    = "\n".join(alerts)
    msg     = MIMEText(f"DB Health Alerts — {datetime.now()}\n\n{body}")
    msg["Subject"] = "🚨 Database Health Alert"
    msg["From"]    = ALERT_EMAIL
    msg["To"]      = ALERT_EMAIL

    with smtplib.SMTP("localhost") as smtp:
        smtp.send_message(msg)
    print("Alert email sent.")

if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    all_alerts = check_connections(cur) + check_long_queries(cur)

    if all_alerts:
        for a in all_alerts:
            print(a)
        send_alert(all_alerts)
    else:
        print(f"✅ [{datetime.now()}] All checks passed.")

    cur.close()
    conn.close()
