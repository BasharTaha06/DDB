package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func (n *Node) forwardCommand(cmd CommandMsg) {
	var leaderUrl string
	for _, p := range n.Peers {
		if strings.Contains(p, n.LeaderID) {
			leaderUrl = p
			break
		}
	}
	if leaderUrl == "" {
		leaderUrl = "http://localhost:" + n.LeaderID
	}

	url := leaderUrl + cmd.Path
	req, _ := http.NewRequest(cmd.Method, url, bytes.NewBuffer(cmd.Body))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		cmd.Reply <- CommandReply{StatusCode: 500, Body: []byte(`{"error": "failed to forward"}`)}
		return
	}
	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)
	cmd.Reply <- CommandReply{StatusCode: resp.StatusCode, Body: respBody}
}

func (n *Node) processCommand(cmd CommandMsg) {
	var req map[string]interface{}
	if len(cmd.Body) > 0 {
		json.Unmarshal(cmd.Body, &req)
	}

	var err error
	var result interface{}

	dbName, _ := req["db"].(string)
	tableName, _ := req["table"].(string)

	switch cmd.Path {
	case "/db/create":
		err = n.Engine.CreateDB(dbName)
	case "/db/drop":
		err = n.Engine.DropDB(dbName)
	case "/table/create":
		attrs := []string{}
		if a, ok := req["attributes"].([]interface{}); ok {
			for _, v := range a {
				attrs = append(attrs, v.(string))
			}
		}
		err = n.Engine.CreateTable(dbName, tableName, attrs)
	case "/table/drop":
		err = n.Engine.DropTable(dbName, tableName)
	case "/query/insert":
		record, _ := req["record"].(map[string]interface{})
		err = n.Engine.Insert(dbName, tableName, record)
	case "/query/select":
		query, _ := req["query"].(map[string]interface{})
		result, err = n.Engine.Select(dbName, tableName, query)
	case "/query/update":
		query, _ := req["query"].(map[string]interface{})
		update, _ := req["update"].(map[string]interface{})
		result, err = n.Engine.Update(dbName, tableName, query, update)
	case "/query/delete":
		query, _ := req["query"].(map[string]interface{})
		result, err = n.Engine.Delete(dbName, tableName, query)
	default:
		cmd.Reply <- CommandReply{StatusCode: 404, Body: []byte(`{"error": "not found"}`)}
		return
	}

	// Replicate writes to followers if successful
	isWrite := cmd.Path != "/query/select"
	if err == nil && isWrite && n.State == Leader {
		n.replicateToFollowers(cmd)
	}

	if err != nil {
		cmd.Reply <- CommandReply{StatusCode: 400, Body: []byte(fmt.Sprintf(`{"error": "%s"}`, err.Error()))}
	} else {
		resp := map[string]interface{}{"success": true}
		if result != nil {
			resp["data"] = result
		}
		respBytes, _ := json.Marshal(resp)
		cmd.Reply <- CommandReply{StatusCode: 200, Body: respBytes}
	}
}

func (n *Node) replicateToFollowers(cmd CommandMsg) {
	for _, peer := range n.Peers {
		go func(p string) {
			url := p + cmd.Path
			req, _ := http.NewRequest(cmd.Method, url, bytes.NewBuffer(cmd.Body))
			req.Header.Set("X-Internal-Replication", "true")
			client := &http.Client{}
			client.Do(req)
		}(peer)
	}
}
