#!/usr/bin/env python3
import os, sys, requests

def main():
    host = os.getenv('CLICKHOUSE_HTTP_HOST', 'clickhouse')
    port = os.getenv('CLICKHOUSE_HTTP_PORT', '8123')
    user = os.getenv('CH_USER') or os.getenv('CLICKHOUSE_USER') or os.getenv('CLICKHOUSE_DEFAULT_USER', 'default')
    password = os.getenv('CH_PASSWORD') or os.getenv('CLICKHOUSE_PASSWORD') or os.getenv('CLICKHOUSE_DEFAULT_PASSWORD', '')
    url = f"http://{host}:{port}/"
    headers = {}
    if user:
        headers['X-ClickHouse-User'] = user
    if password:
        headers['X-ClickHouse-Key'] = password
    try:
        r = requests.post(url, data='SELECT 1', headers=headers, timeout=10)
        if r.status_code == 200 and r.text.strip() == '1':
            print('AUTH_OK')
            return 0
        else:
            print('AUTH_FAIL', r.status_code, r.text[:200])
            return 1
    except Exception as e:
        print('AUTH_ERROR', str(e))
        return 2

if __name__ == '__main__':
    sys.exit(main())
