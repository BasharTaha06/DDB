# Distributed Database Implementation Plan

This plan details the architecture and implementation of a custom distributed database in Go, addressing all your requirements.

## Architecture & Design

We will build a simple HTTP-based distributed database with leader election (Master/Slave architecture) and replication.

### Node Roles
- **Master (Leader)**: Handles all write requests (inserts, updates, deletes, table creation/deletion) and replicates them to slaves. Only the master can create/drop databases and tables.
- **Slave (Follower)**: Handles read queries (select/search) directly. If it receives a write query, it will either reject it with a redirect to the Master or forward it to the Master.
- **Candidate**: A node state during leader election when the Master goes down.

### Communication
- Nodes will communicate over **HTTP**.
- Each node runs on a different port (e.g., 8081, 8082, 8083) to simulate a network, or can be deployed on different machines.
- **Heartbeats**: The Master sends periodic ping requests to Slaves to assert its leadership.
- **Election**: If Slaves stop receiving heartbeats, they transition to Candidate state, vote for a new Master, and the system continues.

### Storage Engine
- Data will be stored independently on each node's local disk using JSON files to keep it simple but persistent.
- Directory structure per node: `data_<node_id>/<db_name>/<table_name>.json`.

### Features mapping
- **Create DB / Create Table**: Handled by Master, metadata replicated to Slaves.
- **Queries (Select/Insert/Update/Delete)**: Handled via REST API endpoints. Writes replicated.
- **Drop DB**: Handled by Master only.
- **Replication**: Master logs write operations and broadcasts them to all healthy Slaves.
- **Fault Tolerance**: Standard heartbeat and voting mechanism. If Node 1 (Master) dies, Node 2 and Node 3 will elect a new Master.

---

## User Review Required

> [!WARNING]
> **Write Forwarding vs. Redirection**
> The requirements say "All nodes (masters and slaves) can do some queries... select, insert". When a user sends an `insert` to a Slave, should the Slave **forward** it automatically to the Master behind the scenes, or should it **reject** it and tell the user the address of the Master? I will implement **Write Forwarding** (Slave automatically forwards the request to the Master) for a smoother user experience, unless you prefer otherwise.

> [!IMPORTANT]
> **Dependencies**
> Should this be implemented using purely the Go standard library (for a learning/academic project), or can we use established libraries like `github.com/hashicorp/raft` for fault tolerance, and `gin` for HTTP routing? 
> **My proposal**: I will build the HTTP routing and basic leader election from scratch using only the **Standard Library (`net/http`, `encoding/json`, etc.)** to keep the project self-contained and demonstrate core concepts.

---

## Proposed Changes

### Core Network and Server

#### [NEW] [main.go](file:///c:/Users/Ms/Desktop/DDB/main.go)
Entry point for the application. Parses command line arguments (node ID, port, peers) and starts the node.

#### [NEW] [server/node.go](file:///c:/Users/Ms/Desktop/DDB/server/node.go)
Contains the `Node` struct and state machine (Leader/Follower). Handles heartbeats, leader election voting, and HTTP route definitions.

### Storage Engine

#### [NEW] [storage/engine.go](file:///c:/Users/Ms/Desktop/DDB/storage/engine.go)
Handles independent data storage. Creates directories for DBs, JSON files for tables, and performs read/write operations to disk.
- `CreateDatabase(name)`
- `DropDatabase(name)`
- `CreateTable(dbName, tableName, attributes)`
- `Insert(dbName, tableName, record)`
- `Select(dbName, tableName, query)`
...etc.

### Replication & Queries

#### [NEW] [replication/log.go](file:///c:/Users/Ms/Desktop/DDB/replication/log.go)
Handles direct synchronous replication from Master to Slaves over HTTP.

#### [NEW] [api/handlers.go](file:///c:/Users/Ms/Desktop/DDB/api/handlers.go)
HTTP handlers parsing incoming queries and routing them to the storage engine (if Master or Read query) or forwarding them to the Master (if Write query on Slave).

---

## Verification Plan

### Automated Tests
- I will write a `test_cluster.ps1` script to launch 3 nodes locally on ports 8081, 8082, 8083.
- Use `Invoke-WebRequest` commands to:
  1. Create DB and Tables on the Master.
  2. Insert records via Master and Slaves (to test forwarding).
  3. Query records from Slaves (to test replication).
  4. Kill the Master process.
  5. Verify a new Master is elected and system accepts new writes.
  6. Attempt to Drop DB from a Slave (should fail/forward) and from Master (should succeed).
