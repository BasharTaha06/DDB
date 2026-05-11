package server

import (
	"bytes"
	"ddb/storage"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type Node struct {
	ID    string
	Port  string
	Peers []string // list of other node addresses (e.g., "http://localhost:8082")

	State       NodeState
	CurrentTerm int
	VotedFor    string
	LeaderID    string

	Engine *storage.Engine

	// Channels for thread-safe state management
	HeartbeatC  chan HeartbeatMsg
	RequestVote chan VoteRequestMsg
	CommandC    chan CommandMsg

	mu sync.RWMutex
}

type HeartbeatMsg struct {
	Term     int
	LeaderID string
	Reply    chan bool
}

type VoteRequestMsg struct {
	Term         int
	CandidateID  string
	Reply        chan bool
}

type CommandMsg struct {
	Type          string // "client_request"
	Method        string // "POST", "GET"
	Path          string
	Body          []byte
	IsReplication bool
	Reply         chan CommandReply
}

type CommandReply struct {
	StatusCode int
	Body       []byte
}

func NewNode(id, port string, peers []string) *Node {
	return &Node{
		ID:          id,
		Port:        port,
		Peers:       peers,
		State:       Follower,
		Engine:      storage.NewEngine(id),
		HeartbeatC:  make(chan HeartbeatMsg),
		RequestVote: make(chan VoteRequestMsg),
		CommandC:    make(chan CommandMsg),
	}
}

func (n *Node) Run() {
	go n.eventLoop()
	n.startHTTPServer()
}

func (n *Node) eventLoop() {
	for {
		switch n.State {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func randomTimeout() time.Duration {
	return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
}

func (n *Node) runFollower() {
	log.Printf("Node %s entering Follower state", n.ID)
	timeout := time.After(randomTimeout())

	for n.State == Follower {
		select {
		case hb := <-n.HeartbeatC:
			if hb.Term >= n.CurrentTerm {
				n.CurrentTerm = hb.Term
				n.LeaderID = hb.LeaderID
				timeout = time.After(randomTimeout()) // reset timeout
				hb.Reply <- true
			} else {
				hb.Reply <- false
			}

		case vr := <-n.RequestVote:
			if vr.Term > n.CurrentTerm {
				n.CurrentTerm = vr.Term
				n.VotedFor = vr.CandidateID
				timeout = time.After(randomTimeout())
				vr.Reply <- true
			} else {
				vr.Reply <- false
			}

		case cmd := <-n.CommandC:
			// Follower handling command
			if cmd.IsReplication || cmd.Path == "/query/select" {
				n.processCommand(cmd)
			} else if n.LeaderID == "" {
				cmd.Reply <- CommandReply{StatusCode: 503, Body: []byte(`{"error": "no leader"}`)}
			} else if cmd.Path == "/db/drop" || cmd.Path == "/db/create" || cmd.Path == "/table/create" {
				// Master only actions
				cmd.Reply <- CommandReply{StatusCode: 403, Body: []byte(fmt.Sprintf(`{"error": "Action denied. Rule: Only Master can Create/Drop DB and Create Tables. Current Master is %s"}`, n.LeaderID))}
			} else if cmd.Path == "/query/raw" && strings.Contains(strings.ToUpper(string(cmd.Body)), "DROP DATABASE") {
				cmd.Reply <- CommandReply{StatusCode: 403, Body: []byte(`{"error": "DROP DATABASE queries are only allowed on the Master node."}`)}
			} else {
				// All nodes can query (forward write to leader)
				n.forwardCommand(cmd)
			}

		case <-timeout:
			log.Printf("Node %s heartbeat timeout, transitioning to Candidate", n.ID)
			n.State = Candidate
		}
	}
}

func (n *Node) runCandidate() {
	n.CurrentTerm++
	n.VotedFor = n.ID
	votes := 1 // Vote for self
	log.Printf("Node %s entering Candidate state (Term %d)", n.ID, n.CurrentTerm)

	// Send RequestVote to peers
	for _, peer := range n.Peers {
		go func(p string) {
			reqBody, _ := json.Marshal(map[string]interface{}{
				"Term":        n.CurrentTerm,
				"CandidateID": n.ID,
			})
			resp, err := http.Post(fmt.Sprintf("%s/raft/vote", p), "application/json", bytes.NewBuffer(reqBody))
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					var result map[string]bool
					json.NewDecoder(resp.Body).Decode(&result)
					if result["granted"] {
						// This is a bit unsafe without a channel, but simplifed for brevity
						// In full implementation we'd use a channel to collect votes safely
						n.mu.Lock()
						votes++
						n.mu.Unlock()
					}
				}
			}
		}(peer)
	}

	timeout := time.After(randomTimeout())
	for n.State == Candidate {
		n.mu.RLock()
		currentVotes := votes
		n.mu.RUnlock()

		clusterSize := len(n.Peers) + 1 // total nodes including self
		majority := clusterSize/2 + 1
		if currentVotes >= majority {
			log.Printf("Node %s won election, becoming Leader", n.ID)
			n.State = Leader
			n.LeaderID = n.ID
			return
		}

		select {
		case hb := <-n.HeartbeatC:
			if hb.Term >= n.CurrentTerm {
				n.State = Follower
				n.CurrentTerm = hb.Term
				n.LeaderID = hb.LeaderID
				hb.Reply <- true
				return
			}
			hb.Reply <- false

		case vr := <-n.RequestVote:
			if vr.Term > n.CurrentTerm {
				n.CurrentTerm = vr.Term
				n.VotedFor = vr.CandidateID
				n.State = Follower
				vr.Reply <- true
				return
			}
			vr.Reply <- false

		case cmd := <-n.CommandC:
			cmd.Reply <- CommandReply{StatusCode: 503, Body: []byte(`{"error": "election in progress"}`)}

		case <-timeout:
			// Restart election
			return
		case <-time.After(100 * time.Millisecond):
			// check votes loop
		}
	}
}

func (n *Node) runLeader() {
	log.Printf("Node %s entering Leader state (Term %d)", n.ID, n.CurrentTerm)

	// Heartbeat ticker
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for n.State == Leader {
		select {
		case <-ticker.C:
			n.sendHeartbeats()

		case hb := <-n.HeartbeatC:
			if hb.Term > n.CurrentTerm {
				n.State = Follower
				n.CurrentTerm = hb.Term
				n.LeaderID = hb.LeaderID
				hb.Reply <- true
				return
			}
			hb.Reply <- false

		case vr := <-n.RequestVote:
			if vr.Term > n.CurrentTerm {
				n.State = Follower
				n.CurrentTerm = vr.Term
				n.VotedFor = vr.CandidateID
				vr.Reply <- true
				return
			}
			vr.Reply <- false

		case cmd := <-n.CommandC:
			n.processCommand(cmd)
		}
	}
}

func (n *Node) sendHeartbeats() {
	for _, peer := range n.Peers {
		go func(p string) {
			reqBody, _ := json.Marshal(map[string]interface{}{
				"Term":     n.CurrentTerm,
				"LeaderID": n.ID,
			})
			http.Post(fmt.Sprintf("%s/raft/heartbeat", p), "application/json", bytes.NewBuffer(reqBody))
		}(peer)
	}
}
