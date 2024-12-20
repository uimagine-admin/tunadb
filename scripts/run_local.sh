#!/bin/bash

DATA_DIR="./internal/db/internal/data"

# Function to create the data directory and files if missing
setup_data_directory() {
  NODE_ID=$1
  NODE_FILE="${DATA_DIR}/${NODE_ID}.json"

  # Create data directory if it doesn't exist
  if [ ! -d "$DATA_DIR" ]; then
    echo "Creating data directory at $DATA_DIR"
    mkdir -p "$DATA_DIR"
  fi

  # Create a JSON file for the node if it doesn't exist
  if [ ! -f "$NODE_FILE" ]; then
    echo "Creating data file for $NODE_ID at $NODE_FILE"
    echo "{}" > "$NODE_FILE"
  fi
}

# Function to start a node
start_node() {
  NODE_NAME=$1
  PORT=$2
  PEER_NODES=$3
  ID=$4
  MAX_BATCH_SIZE=$5

  setup_data_directory "$ID"

  echo "Starting $NODE_NAME on port $PORT"
  NODE_NAME=$NODE_NAME \
  PEER_NODES=$PEER_NODES \
  ID=$ID \
  MAX_BATCH_SIZE=$MAX_BATCH_SIZE \
  go run ./cmd/node/node.go &
}

# Function to start the client
start_client() {
  CLIENT_NAME=$1
  PEER_ADDRESS=$2

  echo "Starting $CLIENT_NAME"
  NODE_NAME=$CLIENT_NAME \
  PEER_ADDRESS=$PEER_ADDRESS \
  go run ./cmd/node/client.go &
}

# Start nodes
start_node "cassandra-node1" "50051" "cassandra-node2:50052,cassandra-node3:50053,cassandra-node4:50054" "node-1" "150"
start_node "cassandra-node2" "50052" "cassandra-node1:50051,cassandra-node3:50053,cassandra-node4:50054" "node-2" "150"
start_node "cassandra-node3" "50053" "cassandra-node1:50051,cassandra-node2:50052,cassandra-node4:50054" "node-3" "150"
start_node "cassandra-node4" "50054" "cassandra-node1:50051,cassandra-node2:50052,cassandra-node3:50053" "node-4" "150"

# Start the client
start_client "Client" "cassandra-node1:50051"

# Wait for all background processes to finish
wait
