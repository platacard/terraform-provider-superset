#!/bin/sh
# Registers the PostgreSQL of the compose file as the database connection "tf-review pg" in
# both Superset instances, once they answer. Run it after `docker compose up -d`.
set -eu

for port in 8088 8089; do
  host="http://localhost:$port"
  printf 'waiting for %s ' "$host"
  for _ in $(seq 1 90); do
    if curl -fsS "$host/health" > /dev/null 2>&1; then break; fi
    printf '.'
    sleep 5
  done
  echo
  python3 - "$host" <<'PY'
import http.cookiejar, json, sys, urllib.parse, urllib.request, urllib.error

host = sys.argv[1]
jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))


def call(method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(host + path, data=data, method=method, headers={"Content-Type": "application/json", "Referer": host, **(headers or {})})
    try:
        with opener.open(request, timeout=60) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors="replace")[:300]


status, login = call("POST", "/api/v1/security/login", {"username": "admin", "password": "admin", "provider": "db", "refresh": True})
if status != 200:
    sys.exit(f"{host}: login failed: {status} {login}")
auth = {"Authorization": "Bearer " + login["access_token"]}
status, csrf = call("GET", "/api/v1/security/csrf_token/", headers=auth)
auth["X-CSRFToken"] = csrf["result"]

query = urllib.parse.quote("(filters:!((col:database_name,opr:eq,value:'tf-review pg')))")
status, existing = call("GET", "/api/v1/database/?q=" + query, headers=auth)
if status == 200 and existing["count"] > 0:
    print(f"{host}: the connection 'tf-review pg' exists with id {existing['result'][0]['id']}")
    sys.exit(0)

status, created = call("POST", "/api/v1/database/", {"database_name": "tf-review pg", "sqlalchemy_uri": "postgresql+psycopg2://pguser:pgpass@tf-review-pg:5432/pgdb", "expose_in_sqllab": True}, headers=auth)
if status != 201:
    sys.exit(f"{host}: creating the connection failed: {status} {created}")
print(f"{host}: created the connection 'tf-review pg' with id {created['id']}")
PY
done
