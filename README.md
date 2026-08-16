# raft-kv

A distributed key-value store built from scratch in Python, implementing the [Raft consensus algorithm](https://raft.github.io/raft.pdf) — leader election and log replication — for the purpose of actually understanding consensus deeply enough to defend it in an interview, not just getting a demo working.

**Status: complete.** Leader election, log replication, and a client that transparently discovers the leader all work end-to-end, verified live — including killing the leader process mid-session and confirming the cluster recovers with no data loss. Scope decisions (what's deliberately excluded and why) are documented below rather than left unstated.

## Architecture

Each node is an independent OS process (`python node.py --port <PORT> --id <ID>`) with no shared memory. Nodes communicate exclusively over HTTP (via FastAPI + `requests`), the same way real Raft nodes communicate over RPC. State is entirely in-memory.

```
python node.py --port 8000 --id node1
python node.py --port 8001 --id node2
python node.py --port 8002 --id node3
```

Each node's peer list is currently hardcoded per `--id` inside `node.py` (see "Not implemented" below for why).

## What's implemented

**Leader election**
- Randomized election timeouts, candidate/leader state transitions, majority vote counting.
- A leader steps down immediately if it discovers a higher term, whether learned via an incoming `RequestVote` or from a heartbeat response — a node can never remain leader once it's been superseded.
- Vote granting includes the Raft log-freshness check: a candidate only wins a vote if its log is at least as up-to-date as the voter's (comparing term first, log length only as a tiebreaker). This prevents a node with a stale log from being elected and overwriting already-committed data.
- A freshly-elected leader sends heartbeats immediately, rather than waiting for the next scheduled tick, to establish authority before any follower's timer can fire.

**Log replication**
- The leader accepts client writes (`POST /write`) and appends them to its own log.
- Heartbeats double as `AppendEntries` RPCs, carrying real log entries with `prevLogIndex`/`prevLogTerm` for the log-matching check.
- Followers reject entries that don't fit their log at the claimed position; on mismatch the leader backs up `nextIndex` and retries with an earlier entry, per the standard Raft recovery mechanism.
- On conflict, followers truncate their log from the conflicting point onward and accept the leader's version.
- The leader advances `commitIndex` once an entry from its **current** term is replicated to a majority (the Raft §5.4.2 safety rule — entries from older terms are never committed directly, only carried forward once a current-term entry commits).
- Committed entries are applied to `kv_store` — on the leader immediately upon commit, on followers one heartbeat cycle later once they learn the new `commitIndex`.

**Client** (`client.py`)
- The client holds the addresses of all known nodes but never assumes which one is leader — it discovers this by trying each node in turn.
- `python client.py write <key> <value>` — tries each node's `/write`; a follower correctly refusing (`not_leader`) or an unreachable node just moves the client on to the next one.
- `python client.py read <key>` — same retry pattern against `/read/<key>`; any node can serve a read (see the staleness note below).
- Both commands wrap the per-node scan in an outer retry loop (up to 5 rounds, 2s apart) — a single scan can land mid-election, when no node is leader yet, which isn't a real failure. This mirrors how real Raft client libraries (e.g. etcd's) behave: they wait out leadership changes rather than surfacing them as errors.

**Testing**
- Manual: run 3+ nodes in separate terminals, inspect state via `GET /debug`; drive writes/reads through `client.py`.
- Automated: `tests/test_cluster.py` spins up a real 3-node cluster as subprocesses and drives it over HTTP — waits for a leader, writes a key, confirms replication to all nodes, kills the leader, confirms a new one is elected. See "Known limitation" below.

## Not implemented (explicit scope decisions)

These are deliberately out of scope for this project, not oversights:

- **Persistence.** Everything is in-memory; a crashed node loses its entire log and state. Real Raft requires persisting `currentTerm`, `votedFor`, and the log to disk before responding to RPCs. Adding this is straightforward in principle but wasn't the point of this exercise — the goal was understanding consensus logic, not building a WAL.
- **Dynamic cluster membership.** Adding/removing nodes at runtime requires *joint consensus* (Raft §6) — membership changes propagated through the log itself as special entries, with quorums computed against both old and new configurations during a transition period. This is a legitimate, separate subsystem on top of a working replicated log, not a small addition, and is left as a stretch goal.
- **Snapshotting / log compaction.** No mechanism to compact an ever-growing log or ship snapshots to slow followers.
- **Server-side write redirect.** `POST /write` on a non-leader returns `{"success": false, "error": "not_leader"}` rather than forwarding the request itself or returning the current leader's address. `client.py` solves the practical problem client-side instead (retry across known nodes until one accepts) — a legitimate alternative to server-side redirect, just not the only way to handle it.
- **Chaos / partition testing.** Testing so far covers leader-kill recovery; network partition simulation (e.g. isolating the leader from a majority) hasn't been exercised.
- **Stale follower reads.** `/read` can be answered by any node, including one that's lagging slightly behind the leader (by up to one heartbeat cycle). No read-index or lease mechanism was added to guarantee linearizable reads — reads are "probably current," not strictly guaranteed to be.

## Known limitation

`tests/test_cluster.py` is not reliably green on every run. On a dev machine with several Python/uvicorn processes competing for CPU, election convergence can occasionally take much longer than expected, and on long-running processes leadership churn has been observed to persist well beyond what a couple of split-vote rounds would explain. The root cause wasn't fully isolated — candidate causes explored include CPU scheduling jitter and the timeout randomization window being too narrow, but neither fully explains sustained multi-minute churn seen in one live session. `client.py`'s retry-with-backoff logic was added specifically to make the system usable despite this, and does so successfully. Revisiting this with proper timestamped tracing is the natural next step if this project is picked up again.
