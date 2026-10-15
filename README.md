# miniraft

A fault-tolerant Distributed Key-Value Store and Lock Manager built from scratch in Go implementing the **Raft Consensus Algorithm** (Leader Election, Heartbeats, Log Replication, and Split-Brain Prevention) over TCP RPC.

## Overview

<!-- CUSTOMIZE: Rewrite this paragraph in your own words to explain why you built this project -->
Storing state on a single server creates a single point of failure, while replicating state across multiple servers introduces the risk of "split-brain" and data divergence during network partitions or crashes. I built `miniraft` to implement the core Raft Consensus Algorithm from scratch—demonstrating how a cluster of independent nodes elects a single leader, replicates an ordered state machine log across a quorum, and survives abrupt `kill -9` leader failures with zero data loss.

---

## Distributed Systems Fundamentals Covered

### 1. Leader Election & Randomized Timeouts (`RequestVote` RPC)
<!-- CUSTOMIZE: Explain in your own words how Followers become Candidates and why randomized timeouts prevent infinite split votes -->
Every node runs as a **Follower**, **Candidate**, or **Leader** within monotonically increasing election rounds called **Terms**:
* Each Follower runs a background election timer randomized between **300ms and 600ms**.
* If a Follower does not receive a heartbeat from a Leader before its timer expires, it increments `currentTerm`, transitions to **Candidate**, votes for itself, and broadcasts `RequestVote` RPCs over TCP to all peers.
* **Election Safety:** A voter only grants its vote if the Candidate's term is at least as large as its own *and* the Candidate's replicated log (`lastLogTerm`, `lastLogIndex`) is at least as up-to-date as the voter's log.

### 2. Quorum Math & Split-Brain Prevention
To win an election or commit a log entry, a node must secure a strict majority (**Quorum**) of the cluster:

$$\text{Quorum} = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

* In a **3-node cluster**, Quorum = **2 nodes** (tolerates **1** simultaneous node failure).
* In a **5-node cluster**, Quorum = **3 nodes** (tolerates **2** simultaneous node failures).
* **Why Split-Brain is Impossible:** Any two majorities of the same cluster must overlap by at least one node. Because each node can vote at most once per Term, two Candidates can never simultaneously achieve a Quorum in the same Term, and an isolated minority partition automatically refuses client writes.

### 3. Log Replication & Distributed State Machine (`AppendEntries` RPC)
<!-- CUSTOMIZE: Explain in your own words how a write is not applied immediately, but only after a majority of nodes write it to their logs -->
Client mutations (`SET`, `LOCK`, `UNLOCK`) are never applied directly to memory on receipt. Instead, they go through a two-phase consensus flow:

```text
Client ──► 1. HTTP Request (/set or /lock)
               │
               ▼
       [ Leader Node ]  ──► Appends entry to local uncommitted log[]
               │
               ├──► 2. AppendEntries RPC (TCP) ──► [ Follower A ] (Verifies PrevLogIndex & appends)
               └──► 2. AppendEntries RPC (TCP) ──► [ Follower B ] (Verifies PrevLogIndex & appends)
               │
               ▼
       3. Quorum (2/3) Acknowledges Entry
               │
               ▼
       4. Leader advances commitIndex ──► Applies command to KV & Lock State Machine
```

### 4. Idempotency (`req_id` Deduplication)
In a distributed network, a Leader might commit a client request and crash one millisecond before sending the HTTP response back. If the client retries the same request against the new Leader, `miniraft` checks the entry's unique `req_id` against its replicated `appliedReqs` table so retried operations are never executed twice.

---

## Project Structure

```text
miniraft/
├── .gitignore    # Excludes compiled binary and log files
├── go.mod        # Go module definition
├── main.go       # Raft state machine, TCP RPCs (RequestVote/AppendEntries), and HTTP API
├── Makefile      # Automation for building, starting a 3-node cluster, and status checks
└── README.md     # Project documentation
```

---

## Getting Started & Live `kill -9` Failover Demo

### Prerequisites
* Linux or Windows **WSL 2**
* **Go** 1.20+, `make`, and `curl`

### 1. Start a 3-Node Local Raft Cluster
```bash
make cluster
```
Check the cluster state to see which node won the initial election:
```bash
make status
```
```text
Node ID: 1 | PID: 21405 | Role: LEADER   | Term: 1 | Leader: Node 1 | CommitIndex: 0 | LogLen: 0
Node ID: 2 | PID: 21406 | Role: FOLLOWER | Term: 1 | Leader: Node 1 | CommitIndex: 0 | LogLen: 0
Node ID: 3 | PID: 21407 | Role: FOLLOWER | Term: 1 | Leader: Node 1 | CommitIndex: 0 | LogLen: 0
```

### 2. Replicate Key-Value Data & Acquire a Distributed Lock
Send writes to the Leader's HTTP port (`9001` for Node 1, `9002` for Node 2, `9003` for Node 3):
```bash
# Replicate a Key-Value pair across the cluster
curl "http://localhost:9001/set?key=balance&val=50000&req_id=tx101"
# Output: SUCCESS: Committed at log index 1 (Term 1)

# Acquire a Distributed Lock across the cluster
curl "http://localhost:9001/lock?res=db_write&owner=service_A&req_id=tx102"
# Output: SUCCESS: Committed at log index 2 (Term 1)
```

### 3. Kill the Leader (`kill -9`) and Verify Automatic Failover
Terminate the current Leader process abruptly using its PID from `make status`:
```bash
kill -9 21405
```
Within ~400ms, the surviving Followers time out, increment the Term, and elect a new Leader:
```bash
make status
```
```text
Node 1: DEAD
Node ID: 2 | PID: 21406 | Role: LEADER   | Term: 2 | Leader: Node 2 | CommitIndex: 2 | LogLen: 2
Node ID: 3 | PID: 21407 | Role: FOLLOWER | Term: 2 | Leader: Node 2 | CommitIndex: 2 | LogLen: 2
```

### 4. Verify Zero Data Loss & Lock Safety on the New Leader
```bash
# Query the key from the new Leader (Node 2)
curl "http://localhost:9002/get?key=balance"
# Output: [Node 2 | LEADER | Term 2] balance = 50000

# Attempt to acquire the already-held lock from another client
curl "http://localhost:9002/lock?res=db_write&owner=service_B"
# Output: LOCK DENIED: Resource 'db_write' is already locked by 'service_A'
```

### 5. Stop the Cluster
```bash
make stop
```