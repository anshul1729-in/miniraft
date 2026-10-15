package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// LogEntry represents a single replicated state machine command
type LogEntry struct {
	Index   int
	Term    int
	Op      string // "SET", "LOCK", "UNLOCK"
	Key     string
	Value   string
	ReqID   string // Ensures idempotency across client retries
}

// RaftNode holds the complete Raft state and KV/Lock state machine
type RaftNode struct {
	mu sync.Mutex

	id    int
	peers []int // IDs of all nodes in the cluster (1..N)

	// Persistent-style Raft state
	currentTerm int
	votedFor    int
	log         []LogEntry // 1-indexed (log[0] is a dummy entry)

	// Volatile Raft state
	role          Role
	leaderID      int
	commitIndex   int
	lastApplied   int
	lastHeartbeat time.Time

	// Leader-only volatile state
	nextIndex  map[int]int
	matchIndex map[int]int

	// Distributed State Machine (KV Store + Distributed Locks + Idempotency table)
	kvStore     map[string]string
	locks       map[string]string // resource -> owner
	appliedReqs map[string]bool   // ReqID -> true (idempotency check)
}

// --- Raft RPC Structs (Exported for net/rpc over TCP) ---

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func NewRaftNode(id int, peers []int) *RaftNode {
	rn := &RaftNode{
		id:            id,
		peers:         peers,
		currentTerm:   0,
		votedFor:      -1,
		log:           []LogEntry{{Index: 0, Term: 0}}, // Dummy entry at index 0
		role:          Follower,
		leaderID:      -1,
		commitIndex:   0,
		lastApplied:   0,
		lastHeartbeat: time.Now(),
		nextIndex:     make(map[int]int),
		matchIndex:    make(map[int]int),
		kvStore:       make(map[string]string),
		locks:         make(map[string]string),
		appliedReqs:   make(map[string]bool),
	}
	return rn
}

func (rn *RaftNode) lastLogIndexAndTerm() (int, int) {
	last := rn.log[len(rn.log)-1]
	return last.Index, last.Term
}

func (rn *RaftNode) logf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Printf("[Node %d | PID %d | Term %d | %-9s] %s\n",
		rn.id, os.Getpid(), rn.currentTerm, rn.role.String(), msg)
}

// 1. RequestVote RPC Handler (Invoked by Candidates during Leader Election)
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
		rn.leaderID = -1
	}

	reply.Term = rn.currentTerm

	// Election Safety: Check if candidate's log is at least as up-to-date as ours
	myLastIdx, myLastTerm := rn.lastLogIndexAndTerm()
	logUpToDate := (args.LastLogTerm > myLastTerm) ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateID) && logUpToDate {
		rn.votedFor = args.CandidateID
		rn.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		rn.logf("Granted vote to Candidate Node %d for Term %d", args.CandidateID, args.Term)
	} else {
		reply.VoteGranted = false
	}
	return nil
}

// 2. AppendEntries RPC Handler (Heartbeats & Log Replication from Leader)
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > rn.currentTerm || rn.role != Follower {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
	}

	rn.leaderID = args.LeaderID
	rn.lastHeartbeat = time.Now()
	reply.Term = rn.currentTerm

	// Verify log consistency at PrevLogIndex
	if args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return nil
	}

	// Append any new entries (overwriting uncommitted conflicts if present)
	insertIdx := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertIdx + i
		if idx < len(rn.log) {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}

	// Advance commitIndex and apply newly committed entries to the state machine
	if args.LeaderCommit > rn.commitIndex {
		lastIdx := len(rn.log) - 1
		if args.LeaderCommit < lastIdx {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastIdx
		}
		rn.applyCommittedEntriesLocked()
	}

	reply.Success = true
	return nil
}

// Applies committed log entries in order to the in-memory KV and Lock State Machine
func (rn *RaftNode) applyCommittedEntriesLocked() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]

		// Idempotency check: never execute the same client ReqID twice
		if entry.ReqID != "" && rn.appliedReqs[entry.ReqID] {
			continue
		}
		if entry.ReqID != "" {
			rn.appliedReqs[entry.ReqID] = true
		}

		switch entry.Op {
		case "SET":
			rn.kvStore[entry.Key] = entry.Value
			rn.logf("Applied log[%d]: SET %s = %s", entry.Index, entry.Key, entry.Value)
		case "LOCK":
			if _, held := rn.locks[entry.Key]; !held {
				rn.locks[entry.Key] = entry.Value
				rn.logf("Applied log[%d]: LOCK '%s' acquired by '%s'", entry.Index, entry.Key, entry.Value)
			}
		case "UNLOCK":
			if owner, held := rn.locks[entry.Key]; held && owner == entry.Value {
				delete(rn.locks, entry.Key)
				rn.logf("Applied log[%d]: UNLOCK '%s' released by '%s'", entry.Index, entry.Key, entry.Value)
			}
		}
	}
}

// Background loop checking randomized election timeout (300ms - 600ms)
func (rn *RaftNode) electionLoop() {
	for {
		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		time.Sleep(50 * time.Millisecond)

		rn.mu.Lock()
		if rn.role != Leader && time.Since(rn.lastHeartbeat) >= timeout {
			rn.startElectionLocked()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) startElectionLocked() {
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.lastHeartbeat = time.Now()

	term := rn.currentTerm
	lastIdx, lastTerm := rn.lastLogIndexAndTerm()
	quorum := len(rn.peers)/2 + 1
	votes := 1 // Vote for self

	rn.logf("Election timeout! Starting election for Term %d (need %d/%d quorum)...",
		term, quorum, len(rn.peers))

	for _, peerID := range rn.peers {
		if peerID == rn.id {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if callRPC(peer, "RaftNode.RequestVote", args, &reply) {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if reply.Term > rn.currentTerm {
					rn.currentTerm = reply.Term
					rn.role = Follower
					rn.votedFor = -1
					return
				}

				if rn.role == Candidate && rn.currentTerm == term && reply.VoteGranted {
					votes++
					if votes >= quorum {
						rn.becomeLeaderLocked()
					}
				}
			}
		}(peerID)
	}
}

func (rn *RaftNode) becomeLeaderLocked() {
	rn.role = Leader
	rn.leaderID = rn.id
	nextIdx := len(rn.log)
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = nextIdx
		rn.matchIndex[peer] = 0
	}
	rn.matchIndex[rn.id] = nextIdx - 1
	rn.logf("*** WON ELECTION! Became LEADER for Term %d ***", rn.currentTerm)
	go rn.sendHeartbeats()
}

// Leader heartbeat & log replication ticker (runs every 100ms while Leader)
func (rn *RaftNode) heartbeatLoop() {
	for {
		time.Sleep(100 * time.Millisecond)
		rn.mu.Lock()
		isLeader := (rn.role == Leader)
		rn.mu.Unlock()

		if isLeader {
			rn.sendHeartbeats()
		}
	}
}

func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return
	}
	term := rn.currentTerm
	rn.mu.Unlock()

	for _, peerID := range rn.peers {
		if peerID == rn.id {
			continue
		}
		go func(peer int) {
			rn.mu.Lock()
			if rn.role != Leader {
				rn.mu.Unlock()
				return
			}
			nextIdx := rn.nextIndex[peer]
			if nextIdx < 1 {
				nextIdx = 1
			}
			if nextIdx > len(rn.log) {
				nextIdx = len(rn.log)
			}
			prevIdx := nextIdx - 1
			prevTerm := rn.log[prevIdx].Term
			entries := make([]LogEntry, len(rn.log[nextIdx:]))
			copy(entries, rn.log[nextIdx:])

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rn.id,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: rn.commitIndex,
			}
			rn.mu.Unlock()

			var reply AppendEntriesReply
			if callRPC(peer, "RaftNode.AppendEntries", args, &reply) {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if reply.Term > rn.currentTerm {
					rn.currentTerm = reply.Term
					rn.role = Follower
					rn.votedFor = -1
					return
				}
				if rn.role != Leader || rn.currentTerm != term {
					return
				}

				if reply.Success {
					rn.matchIndex[peer] = prevIdx + len(entries)
					rn.nextIndex[peer] = rn.matchIndex[peer] + 1
					rn.updateCommitIndexLocked()
				} else if rn.nextIndex[peer] > 1 {
					rn.nextIndex[peer]--
				}
			}
		}(peerID)
	}
}

// Checks if any log index N > commitIndex is replicated on a Quorum of nodes
func (rn *RaftNode) updateCommitIndexLocked() {
	quorum := len(rn.peers)/2 + 1
	for n := len(rn.log) - 1; n > rn.commitIndex; n-- {
		if rn.log[n].Term != rn.currentTerm {
			continue
		}
		count := 1 // Leader itself has the entry
		for _, peer := range rn.peers {
			if peer != rn.id && rn.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= quorum {
			rn.commitIndex = n
			rn.logf("Quorum (%d/%d) reached for log[%d] -> COMMITTED!", count, len(rn.peers), n)
			rn.applyCommittedEntriesLocked()
			break
		}
	}
}

// Helper to send a TCP RPC with a fast 150ms timeout so dead nodes never block
func callRPC(peerID int, method string, args interface{}, reply interface{}) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", 8000+peerID)
	conn, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(150 * time.Millisecond))

	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply) == nil
}

// Appends a command on the Leader and waits until a Quorum commits it
func (rn *RaftNode) proposeCommand(op, key, val, reqID string) (bool, string) {
	rn.mu.Lock()
	if rn.role != Leader {
		leader := rn.leaderID
		rn.mu.Unlock()
		if leader != -1 {
			return false, fmt.Sprintf("Not Leader! Current Leader is Node %d (use port %d)\n", leader, 9000+leader)
		}
		return false, "No Leader currently elected yet.\n"
	}

	idx := len(rn.log)
	entry := LogEntry{
		Index: idx,
		Term:  rn.currentTerm,
		Op:    op,
		Key:   key,
		Value: val,
		ReqID: reqID,
	}
	rn.log = append(rn.log, entry)
	rn.matchIndex[rn.id] = idx
	rn.logf("Appended log[%d] (%s %s=%s), replicating to followers...", idx, op, key, val)
	rn.mu.Unlock()

	rn.sendHeartbeats()

	// Wait up to 1.5 seconds for Quorum commit
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		rn.mu.Lock()
		committed := rn.commitIndex >= idx
		rn.mu.Unlock()
		if committed {
			return true, fmt.Sprintf("SUCCESS: Committed at log index %d (Term %d)\n", idx, entry.Term)
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false, "ERROR: Timeout waiting for Quorum (is a majority of the cluster alive?)\n"
}

func (rn *RaftNode) startServers() {
	// 1. Start TCP RPC Server for inter-node Raft messages (port 8000 + id)
	rpcServer := rpc.NewServer()
	_ = rpcServer.Register(rn)
	rpcAddr := fmt.Sprintf("127.0.0.1:%d", 8000+rn.id)
	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on RPC port %s: %v", rpcAddr, err))
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcServer.ServeConn(conn)
			}
		}
	}()

	// 2. Start HTTP Server for client commands (port 9000 + id)
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		rn.mu.Lock()
		defer rn.mu.Unlock()
		fmt.Fprintf(w, "Node ID: %d | PID: %d | Role: %s | Term: %d | Leader: Node %d | CommitIndex: %d | LogLen: %d\n",
			rn.id, os.Getpid(), rn.role.String(), rn.currentTerm, rn.leaderID, rn.commitIndex, len(rn.log)-1)
	})

	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		reqID := r.URL.Query().Get("req_id")
		ok, msg := rn.proposeCommand("SET", key, val, reqID)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Fprint(w, msg)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		rn.mu.Lock()
		val, ok := rn.kvStore[key]
		role := rn.role.String()
		term := rn.currentTerm
		rn.mu.Unlock()
		if !ok {
			http.Error(w, fmt.Sprintf("[Node %d | %s | Term %d] Key '%s' not found\n", rn.id, role, term, key), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "[Node %d | %s | Term %d] %s = %s\n", rn.id, role, term, key, val)
	})

	mux.HandleFunc("/lock", func(w http.ResponseWriter, r *http.Request) {
		res := r.URL.Query().Get("res")
		owner := r.URL.Query().Get("owner")
		reqID := r.URL.Query().Get("req_id")

		rn.mu.Lock()
		if curOwner, held := rn.locks[res]; held && curOwner != owner {
			rn.mu.Unlock()
			http.Error(w, fmt.Sprintf("LOCK DENIED: Resource '%s' is already locked by '%s'\n", res, curOwner), http.StatusConflict)
			return
		}
		rn.mu.Unlock()

		ok, msg := rn.proposeCommand("LOCK", res, owner, reqID)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Fprint(w, msg)
	})

	mux.HandleFunc("/unlock", func(w http.ResponseWriter, r *http.Request) {
		res := r.URL.Query().Get("res")
		owner := r.URL.Query().Get("owner")
		ok, msg := rn.proposeCommand("UNLOCK", res, owner, "")
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Fprint(w, msg)
	})

	httpAddr := fmt.Sprintf("127.0.0.1:%d", 9000+rn.id)
	rn.logf("Started! Raft TCP RPC: %s | Client HTTP API: http://%s", rpcAddr, httpAddr)
	go func() {
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			panic(err)
		}
	}()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./miniraft <node_id: 1|2|3> [total_nodes: default 3]")
		os.Exit(1)
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil || id < 1 {
		panic("node_id must be a positive integer (e.g. 1, 2, 3)")
	}

	totalNodes := 3
	if len(os.Args) >= 3 {
		totalNodes, _ = strconv.Atoi(os.Args[2])
	}

	rand.Seed(time.Now().UnixNano() + int64(id)*1000)

	var peers []int
	for i := 1; i <= totalNodes; i++ {
		peers = append(peers, i)
	}

	node := NewRaftNode(id, peers)
	node.startServers()

	go node.electionLoop()
	go node.heartbeatLoop()

	select {} // Keep node running forever
}