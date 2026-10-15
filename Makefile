build:
	go build -o miniraft main.go

# Starts a 3-node Raft cluster in the background and streams their live logs
cluster: build
	@./miniraft 1 3 &
	@./miniraft 2 3 &
	@./miniraft 3 3 &
	@echo "=== 3-Node Raft Cluster Started! (Ports 9001, 9002, 9003) ==="

# Checks the role, PID, and Term of all 3 nodes
status:
	@curl -s http://localhost:9001/status || echo "Node 1: DEAD"
	@curl -s http://localhost:9002/status || echo "Node 2: DEAD"
	@curl -s http://localhost:9003/status || echo "Node 3: DEAD"

# Stops all running miniraft nodes
stop:
	@pkill -9 miniraft || true
	@echo "Cluster stopped."