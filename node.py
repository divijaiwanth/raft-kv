from time import time
import time

from fastapi import FastAPI
import argparse
from pydantic import BaseModel
import uvicorn
import asyncio
import random
import requests

app = FastAPI()

#strcuture of the node
class Node:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = "follower"
        self.kv_store = {}
        self.last_heartbeat = time.time()

#AppendEntriesRequest ( RPC structure )
#for my info RPC stands for Remote Procedure Call, which is a protocol that allows a program to request a service from a program located on another computer in a network. In the context of Raft, AppendEntries is an RPC used by the leader to replicate log entries and to provide a heartbeat signal to followers.
class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list
    leader_commit: int
#the end point is written below

#timerlogic fro checking ( heartbeats )
async def election_timeout_loop():
    while True:
        timeout = random.uniform(1.5,3.0) #this adds randomness to the election timeout
        await asyncio.sleep(timeout)

        elapsed = time.time() - node.last_heartbeat
        if node.state != "leader" and elapsed >= timeout:
            print(f"{node.node_id}: Election Timeout {elapsed:.2f}s, becoming candidate")
            await asyncio.to_thread(start_election)

#election starting logic
def start_election():
    node.state = "candidate"
    node.current_term += 1
    node.voted_for = node.node_id
    node.last_heartbeat = time.time()

    votes_received = 1  # vote for self
    total_nodes = len(PEERS) + 1  # including self
    votes_neded = (total_nodes // 2) + 1

    print(f"{node.node_id}: Starting election for term {node.current_term}, votes needed: {votes_neded}")

    for peer in PEERS:
        try:
            response = requests.post(f"{peer}/request_vote", json= {
                "term": node.current_term,
                "candidate_id": node.node_id,
                "last_log_index": 0,
                "last_log_term": 0
            }, timeout=1)
            data = response.json()
            if data.get("vote_granted"):
                votes_received += 1
                print(f"{node.node_id}: Received vote from {peer}, total votes: {votes_received}")

        except requests.exceptions.RequestException:
            print(f"{node.node_id}: Failed to contact {peer}")
    if node.state == "candidate" and votes_received >= votes_neded:
        node.state = "leader"
        print(f"{node.node_id}: Became leader for term {node.current_term}")
    else:
        node.state = "follower"
        print(f"{node.node_id}: Election failed, reverting to follower")

#after a node becomes the leader the leader needs to send hearbeats
#the logic for that is in this function
def send_heartbeats():
    for peer in PEERS:
        try:
            response = requests.post(f"{peer}/append_entries", json={
                "term": node.current_term,
                "leader_id": node.node_id,
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0
            }, timeout=1)
            data = response.json()
            if data.get("term", node.current_term) > node.current_term:
                node.current_term = data["term"]
                node.state = "follower"
                node.voted_for = None
        except requests.exceptions.RequestException:
            pass # Ignore failures for now

async def heartbeat_loop():
    while True:
        if node.state == "leader":
            send_heartbeats()
        await asyncio.sleep(0.5)  # send heartbeats every 0.5 seconds


class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

node =  None

#startup heartbeat loop
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(election_timeout_loop())
    asyncio.create_task(heartbeat_loop())

@app.get("/debug")
async def debug():
    return {"node_id": node.node_id, "current_term": node.current_term, "voted_for": node.voted_for, "log": node.log, "commit_index": node.commit_index, "last_applied": node.last_applied, "state": node.state, "kv_store": node.kv_store}

@app.post("/request_vote")
async def request_vote(req: RequestVoteRequest):
    # Step 1: reject stale candidates outright

    if req.term < node.current_term:
        return {"term": node.current_term, "vote_granted": False}

    # Step 2: if candidate's term is newer, catch up FIRST —
    # regardless of what happens next
    if req.term > node.current_term:
        node.current_term = req.term
        node.state = "follower"
        node.voted_for = None

    # Step 3: now check if we can vote (log-freshness check omitted for now — log is empty)
    if node.voted_for is None or node.voted_for == req.candidate_id:
        node.voted_for = req.candidate_id
        node.last_heartbeat = time.time()
        return {"term": node.current_term, "vote_granted": True}
    else:
        return {"term": node.current_term, "vote_granted": False}

@app.post("/append_entries")
async def append_entries(req: AppendEntriesRequest):
    if req.term < node.current_term:
        return {"term": node.current_term, "success": False}

    node.current_term = req.term
    node.state = "follower"
    node.voted_for = req.leader_id
    node.last_heartbeat = time.time()

    return {"term": node.current_term, "success": True}

class WriteRequest(BaseModel):
    key: str
    value: str

@app.post("/write")
async def write(req: WriteRequest):
    if node.state != "leader":
        return {"success": False, "error": "not_leader", "leader_id": node.voted_for}

    entry = {"term": node.current_term, "key": req.key, "value": req.value}
    node.log.append(entry)

    return {"success": True, "index": len(node.log) - 1}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", type=str, required=True)
    args = parser.parse_args()

    if args.id == "node1":
        PEERS = ["http://localhost:8001","http://localhost:8002","http://localhost:8003"]
    elif args.id == "node2":
        PEERS = ["http://localhost:8000","http://localhost:8002","http://localhost:8003"]
    elif args.id == "node3":
        PEERS = ["http://localhost:8000","http://localhost:8001","http://localhost:8003"]
    else:
        PEERS = ["http://localhost:8000","http://localhost:8001","http://localhost:8002"]

    node = Node(node_id=args.id)
    uvicorn.run(app, host="0.0.0.0", port=args.port)



#RPC call end point ( AppendEntriesRequest )
