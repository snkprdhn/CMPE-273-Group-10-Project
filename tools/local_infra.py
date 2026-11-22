"""Project-scoped Compose lifecycle and real client smoke checks; stdlib only."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "smart-grid-local-dev"
PREFIX = "local-dev."
STORES = ("registry", "control", "projection", "copilot_audit")
COMPOSE = ["docker", "compose", "--project-name", PROJECT, "--file",
           str(ROOT / "infra/compose/compose.yaml"), "--profile", "faults"]


def compose(*args, data=None, check=True, timeout=180):
    return subprocess.run(COMPOSE + list(args), input=data, text=True,
                          capture_output=True, check=check, timeout=timeout)


def kafka(tool, *args, data=None):
    return compose("exec", "-T", "kafka", f"/opt/kafka/bin/kafka-{tool}.sh",
                   "--bootstrap-server", "kafka:9092", *args, data=data)


def sql(store, query, *, database=None, password=None, port=5432, check=True):
    return compose("exec", "-T", "-e",
                   f"PGPASSWORD={password or store + '_disposable'}", "postgres",
                   "psql", "-X", "-h", "toxiproxy" if port == 15433 else "127.0.0.1",
                   "-p", str(port), "-U", store, "-d", database or f"local_dev_{store}",
                   "-v", "ON_ERROR_STOP=1", "-At", data=query, check=check, timeout=30)


def topics():
    bundle = json.loads((ROOT / "schemas/v1/bundle.json").read_text())
    return [PREFIX + topic for topic in bundle["x-events"]]


def create_topic(topic):
    kafka("topics", "--create", "--if-not-exists", "--topic", topic,
          "--partitions", "3", "--replication-factor", "1", "--config",
          "min.insync.replicas=1", "--config", "retention.ms=86400000")


def health():
    output = compose("ps", "--all", "--format", "json").stdout.strip()
    rows = json.loads(output) if output.startswith("[") else [
        json.loads(line) for line in output.splitlines()]
    states = {row["Service"]: row for row in rows}
    for service in ("kafka", "postgres"):
        if states.get(service, {}).get("Health") != "healthy":
            raise RuntimeError(f"{service} is missing or unhealthy; run make infra-up")
    kafka("topics", "--list")
    for store in STORES:
        if sql(store, "SELECT 1;").stdout.strip() != "1":
            raise RuntimeError(f"{store} query failed")
    print("PASS: Kafka metadata and all four PostgreSQL logins/queries", flush=True)


def up():
    print(compose("up", "--build", "-d", "--wait", "--wait-timeout", "150", "kafka", "postgres").stdout)
    for topic in topics():
        create_topic(topic)
    health()


def consume(topic, token):
    result = kafka("console-consumer", "--topic", topic, "--from-beginning",
                   "--group", PREFIX + "smoke." + uuid4().hex,
                   "--max-messages", "1", "--timeout-ms", "15000",
                   "--property", "print.key=true")
    if result.stdout.strip() != "smoke-key\t" + token:
        raise RuntimeError("Kafka key/value round trip or persisted record mismatch")


def smoke(restart=False):
    health()
    token = uuid4().hex
    topic = PREFIX + "smoke." + token
    create_topic(topic)
    try:
        kafka("console-producer", "--topic", topic, "--producer-property", "acks=all",
              "--property", "parse.key=true", data="smoke-key\t" + token + "\n")
        consume(topic, token)
        for store in STORES:
            sql(store, "CREATE TABLE IF NOT EXISTS infra_smoke "
                "(id text PRIMARY KEY); INSERT INTO infra_smoke VALUES ('" + token + "');")
        if restart:
            # Container removal/recreation proves state is on named volumes.
            print(compose("down", "--timeout", "30").stdout)
            up()
            consume(topic, token)
        for store in STORES:
            if sql(store, f"SELECT id FROM infra_smoke WHERE id='{token}';").stdout.strip() != token:
                raise RuntimeError(f"{store} persistence mismatch")
            wrong = sql(store, "SELECT 1;", password="deliberately-wrong", check=False)
            if wrong.returncode == 0 or "password authentication failed" not in wrong.stderr:
                raise RuntimeError(f"{store}: bad-password rejection was not proven")
            for other in STORES:
                if other != store:
                    denied = sql(store, "SELECT 1;", database=f"local_dev_{other}", check=False)
                    if denied.returncode == 0 or "permission denied for database" not in denied.stderr:
                        raise RuntimeError(f"{store}: cross-store denial was not proven")
        print("PASS: Kafka key/value, four stores, wrong passwords and 12 cross-store denials" +
              ("; Kafka/PostgreSQL survived down/up" if restart else ""), flush=True)
    finally:
        # Only this run's synthetic evidence is removed; no domain tables touched.
        for store in STORES:
            sql(store, f"DELETE FROM infra_smoke WHERE id='{token}';", check=False)
        kafka("topics", "--delete", "--topic", topic)


def proxy_request(path, payload=None):
    request = Request("http://127.0.0.1:18474" + path,
                      data=None if payload is None else json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def fault_smoke():
    health()
    proxy_request("/proxies/postgres", {"enabled": True})
    if sql("registry", "SELECT 1;", port=15433).stdout.strip() != "1":
        raise RuntimeError("Proxy did not forward PostgreSQL query")
    try:
        proxy_request("/proxies/postgres", {"enabled": False})
        if sql("registry", "SELECT 1;", port=15433, check=False).returncode == 0:
            raise RuntimeError("Disabled proxy accepted a query")
    finally:
        proxy_request("/proxies/postgres", {"enabled": True})
    sql("registry", "SELECT 1;", port=15433)
    print("PASS: PostgreSQL proxy forwarding, outage and recovery", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("up", "health", "down", "reset", "smoke",
                        "verify", "faults-up", "faults-down", "fault-smoke"))
    parser.add_argument("--confirm", default="")
    args = parser.parse_args(argv)
    if args.command == "reset" and args.confirm != PROJECT:
        parser.error(f"reset deletes ONLY {PROJECT} containers/network and its two data volumes; "
                     f"repeat with --confirm {PROJECT}")
    if args.command == "up":
        up()
    elif args.command == "health":
        health()
    elif args.command == "down":
        print(compose("down", "--timeout", "30").stdout)
    elif args.command == "reset":
        print(compose("down", "--volumes", "--timeout", "30").stdout)
    elif args.command in ("smoke", "verify"):
        smoke(restart=args.command == "verify")
    elif args.command == "faults-up":
        health()
        print(compose("up", "--build", "-d", "toxiproxy").stdout)
        for attempt in range(20):
            try:
                proxy_request("/proxies/postgres")
                break
            except OSError:
                if attempt == 19:
                    raise
                time.sleep(0.5)
        print("PASS: Toxiproxy API ready", flush=True)
    elif args.command == "faults-down":
        print(compose("rm", "--stop", "--force", "toxiproxy").stdout)
    else:
        fault_smoke()


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        # Do not echo argv: database credentials travel via exec environment.
        detail = error.stderr or "timeout"
        if isinstance(detail, bytes):
            detail = detail.decode(errors="replace")
        print("Dependency command failed: " + detail, file=sys.stderr)
        sys.exit(1)
    except (RuntimeError, OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
