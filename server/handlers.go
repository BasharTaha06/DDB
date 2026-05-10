package server

import (
	//"bytes"
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func (n *Node) startHTTPServer() {
	mux := http.NewServeMux()

	// Node status endpoint
	mux.HandleFunc("/node/status", func(w http.ResponseWriter, r *http.Request) {
		role := "Slave"
		if n.State == Leader {
			role = "Master"
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"role": role,
			"master_id": n.LeaderID,
			"node_id": n.ID,
		})
	})

	// Serve the GUI
	mux.Handle("/", http.FileServer(http.Dir("./public")))

	// Raft endpoints
	mux.HandleFunc("/raft/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		
		replyChan := make(chan bool)
		n.HeartbeatC <- HeartbeatMsg{
			Term:     int(req["Term"].(float64)),
			LeaderID: req["LeaderID"].(string),
			Reply:    replyChan,
		}
		
		success := <-replyChan
		json.NewEncoder(w).Encode(map[string]bool{"success": success})
	})

	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		
		replyChan := make(chan bool)
		n.RequestVote <- VoteRequestMsg{
			Term:        int(req["Term"].(float64)),
			CandidateID: req["CandidateID"].(string),
			Reply:       replyChan,
		}
		
		granted := <-replyChan
		json.NewEncoder(w).Encode(map[string]bool{"granted": granted})
	})

	// Client endpoints
	mux.HandleFunc("/db/create", handleCommand(n))
	mux.HandleFunc("/db/drop", handleCommand(n))
	mux.HandleFunc("/table/create", handleCommand(n))
	mux.HandleFunc("/table/drop", handleCommand(n))
	mux.HandleFunc("/query/insert", handleCommand(n))
	mux.HandleFunc("/query/select", handleCommand(n))
	mux.HandleFunc("/query/update", handleCommand(n))
	mux.HandleFunc("/query/delete", handleCommand(n))
	mux.HandleFunc("/query/raw", handleCommand(n))

	log.Printf("Node %s listening on %s", n.ID, n.Port)
	log.Fatal(http.ListenAndServe(":"+n.Port, mux))
}

func handleCommand(n *Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		replyChan := make(chan CommandReply)
		
		isRep := r.Header.Get("X-Internal-Replication") == "true"

		n.CommandC <- CommandMsg{
			Type:          "client_request",
			Method:        r.Method,
			Path:          r.URL.Path,
			Body:          body,
			IsReplication: isRep,
			Reply:         replyChan,
		}
		
		reply := <-replyChan
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(reply.StatusCode)
		w.Write(reply.Body)
	}
}
