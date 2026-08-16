import argparse
import time

import requests

NODES = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
]

# The election timeout is randomized 3.0-6.0s, so a single pass through
# the node list can easily land mid-election, when no one is leader yet.
# That's not a real failure — retry the whole pass a few times, with a
# short pause between rounds, before actually giving up.
MAX_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 2


def write(key, value):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        for node in NODES:
            try:
                response = requests.post(f"{node}/write", json={"key": key, "value": value}, timeout=2)
                data = response.json()
                if data.get("success"):
                    print(f"Write succeeded via leader {node}: {data}")
                    return
                print(f"Node {node} is not the leader. Trying next node...")
            except requests.exceptions.RequestException:
                print(f"Node {node} is unreachable. Trying next node...")

        if attempt < MAX_ATTEMPTS:
            print(f"No leader found on attempt {attempt}/{MAX_ATTEMPTS} — an election may be in progress, retrying...")
            time.sleep(RETRY_DELAY_SECONDS)

    print("Write failed: no leader found among known nodes after multiple attempts")


def read(key):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        for node in NODES:
            try:
                response = requests.get(f"{node}/read/{key}", timeout=2)
                data = response.json()
                if data.get("error") == "key_not_found":
                    print(f"Node {node} does not have the key '{key}'. Trying next node...")
                    continue

                print(f"Read succeeded via {node}: {data}")
                return
            except requests.exceptions.RequestException:
                print(f"Node {node} is unreachable. Trying next node...")

        if attempt < MAX_ATTEMPTS:
            print(f"Key not found anywhere on attempt {attempt}/{MAX_ATTEMPTS} — retrying...")
            time.sleep(RETRY_DELAY_SECONDS)

    print("Read failed: no node responded with the key after multiple attempts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    write_parser = subparsers.add_parser("write")
    write_parser.add_argument("key")
    write_parser.add_argument("value")

    read_parser = subparsers.add_parser("read")
    read_parser.add_argument("key")

    args = parser.parse_args()

    if args.command == "write":
        write(args.key, args.value)
    elif args.command == "read":
        read(args.key)
