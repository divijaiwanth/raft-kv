"""
Integration test for the Raft cluster: spins up real node.py processes,
talks to them over HTTP exactly like the manual multi-terminal testing does,
and checks that election + replication actually work end to end.

Run with: python tests/test_cluster.py
"""

import os
import subprocess
import sys
import time

import requests

NODE_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "node.py")

NODES = [
    {"id": "node1", "port": 8000},
    {"id": "node2", "port": 8001},
    {"id": "node3", "port": 8002},
]


def get_debug(port):
    try:
        r = requests.get(f"http://localhost:{port}/debug", timeout=2)
        return r.json()
    except requests.exceptions.RequestException:
        return None


def wait_for_leader(ports, timeout=60):
    deadline = time.time() + timeout
    last_states = {}
    while time.time() < deadline:
        last_states = {}
        for port in ports:
            d = get_debug(port)
            if d:
                last_states[port] = (d["state"], d["current_term"])
        leaders = [p for p, (s, t) in last_states.items() if s == "leader"]
        print(f"      ... {last_states}")
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(1)
    raise TimeoutError(f"No single leader within {timeout}s. Last known states: {last_states}")


def start_nodes():
    procs = {}
    for n in NODES:
        log_path = os.path.join(os.path.dirname(__file__), f"_{n['id']}.log")
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            [sys.executable, "-u", NODE_SCRIPT, "--port", str(n["port"]), "--id", n["id"]],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        procs[n["port"]] = proc
    return procs


def stop_all(procs):
    for proc in procs.values():
        if proc.poll() is None:
            proc.terminate()
    for proc in procs.values():
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def main():
    ports = [n["port"] for n in NODES]
    procs = start_nodes()

    try:
        print("[1/4] Waiting for initial leader election...")
        leader_port = wait_for_leader(ports)
        print(f"      Leader elected on port {leader_port}")

        print("[2/4] Writing a key to the leader...")
        resp = requests.post(f"http://localhost:{leader_port}/write", json={"key": "foo", "value": "bar"})
        if not resp.json().get("success"):
            print("      Write failed, dumping full state of all nodes:")
            for port in ports:
                print(f"      port {port}: {get_debug(port)}")
        assert resp.json().get("success"), f"Write to leader failed: {resp.json()}"

        print("      Waiting for replication to followers...")
        time.sleep(2)  # let a couple of heartbeat cycles pass
        for port in ports:
            print(f"      port {port}: {get_debug(port)}")
        for port in ports:
            d = get_debug(port)
            assert d is not None, f"Node on port {port} is not responding"
            assert d["kv_store"].get("foo") == "bar", (
                f"Node on port {port} did not replicate the write. kv_store={d['kv_store']}"
            )
        print("      Replication confirmed on all 3 nodes.")

        print(f"[3/4] Killing the leader (port {leader_port})...")
        procs[leader_port].terminate()
        procs[leader_port].wait(timeout=5)
        remaining_ports = [p for p in ports if p != leader_port]

        print("[4/4] Waiting for a new leader among the remaining nodes...")
        new_leader_port = wait_for_leader(remaining_ports)
        assert new_leader_port != leader_port
        print(f"      New leader elected on port {new_leader_port}")

        print("\nALL CHECKS PASSED")
    finally:
        print("\nCleaning up node processes...")
        stop_all(procs)


if __name__ == "__main__":
    main()
