from time import time

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

#timerlogic fro checking ( heartbeats )
async def election_timeout_loop():
    while True:
        timeout = random.uniform(1.5,3.0) #this adds randomness to the election timeout
        await asyncio.sleep(timeout)

        elapsed = time() - node.last_heartbeat
        if elapsed >= timeout:
            print(f"{node.node_id}: Election Timeout {elapsed:.2f}s, becoming candidate")
        #candidate logic would go here, but for now we just reset print out the timeout and continue


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
        node.voted_for = None

    # Step 3: now check if we can vote (log-freshness check omitted for now — log is empty)
    if node.voted_for is None or node.voted_for == req.candidate_id:
        node.voted_for = req.candidate_id
        return {"term": node.current_term, "vote_granted": True}
    else:
        return {"term": node.current_term, "vote_granted": False}
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", type=str, required=True)
    args = parser.parse_args()

    node = Node(node_id=args.id)

    uvicorn.run(app, host="0.0.0.0", port=args.port)