# raft-kv

A distributed key-value store built from scratch in Python, implementing the [Raft consensus algorithm](https://raft.github.io/raft.pdf) — leader election and log replication — for the purpose of actually understanding consensus deeply enough to defend it in an interview, not just getting a demo working.

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

**Testing**
- Manual: run 3+ nodes in separate terminals, inspect state via `GET /debug`.
- Automated: `tests/test_cluster.py` spins up a real 3-node cluster as subprocesses and drives it over HTTP — waits for a leader, writes a key, confirms replication to all nodes, kills the leader, confirms a new one is elected.

## Not implemented (explicit scope decisions)

These are deliberately out of scope for this project, not oversights:

- **Persistence.** Everything is in-memory; a crashed node loses its entire log and state. Real Raft requires persisting `currentTerm`, `votedFor`, and the log to disk before responding to RPCs. Adding this is straightforward in principle but wasn't the point of this exercise — the goal was understanding consensus logic, not building a WAL.
- **Dynamic cluster membership.** Adding/removing nodes at runtime requires *joint consensus* (Raft §6) — membership changes propagated through the log itself as special entries, with quorums computed against both old and new configurations during a transition period. This is a legitimate, separate subsystem on top of a working replicated log, not a small addition, and is left as a stretch goal.
- **Snapshotting / log compaction.** No mechanism to compact an ever-growing log or ship snapshots to slow followers.
- **Client-facing redirect.** `POST /write` on a non-leader currently just returns `{"success": false, "error": "not_leader"}` rather than forwarding the request or pointing the client at the current leader.
- **Chaos / partition testing.** Testing so far covers leader-kill recovery; network partition simulation (e.g. isolating the leader from a majority) hasn't been exercised.

## Known issue

`tests/test_cluster.py` is not yet reliably green on every run. On a dev machine with several Python/uvicorn processes competing for CPU, election convergence can occasionally take longer than the test's wait window, and the current 3-node cluster's timeout randomization window is narrow enough that split votes sometimes take multiple rounds to resolve. This surfaced two real bugs during development (see git history) and remains useful as a debugging harness even while occasionally flaky.
