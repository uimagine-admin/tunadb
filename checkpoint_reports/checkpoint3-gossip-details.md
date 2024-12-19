# Pull Request: Gossip Protocol Implementation

This pull request introduces the **Gossip Protocol** for fault-tolerant and scalable membership management in the distributed system. Below is a detailed summary of the additions and their functionality.

### Gossip Protocol: Implementation Logic

#### Node Failure Detection
1. **Node Marked as Suspect:**
   - When a node fails to respond during gossip communication, it is marked as *suspect*. 
   - If a node remains in the *suspect* state for a configurable timeout, it transitions to the *dead* state. 
   - This ensures temporary network issues or delays do not immediately classify a node as failed.

2. **Node Marked as Dead:**
   - A node is marked as *dead* under the following conditions:
     - No heartbeat or communication for the duration of the timeout.
     - Received gossip information explicitly marking the node as *dead* with a newer timestamp than the local record.

3. **Node Recovery:**
   - If a node previously marked as *dead* is reported as *alive* with a more recent timestamp, it transitions back to *alive* status and is reintegrated into the consistent hashing ring.

---

#### Gossip Message Merging
1. **Deduplication:**
   - Each gossip message is assigned a unique ID based on the sender's ID and the creation timestamp.
   - Messages already processed are ignored to avoid redundant updates.

2. **Merging Node Data:**
   - Node data is merged based on timestamps:
     - If the incoming message contains newer information, it updates the local record.
     - Conflicts are resolved by prioritizing the *alive* status unless the incoming status is explicitly *dead*.

3. **State Transition Rules:**
   - *Alive → Suspect*: Triggered by missing heartbeat or explicit gossip message.
   - *Suspect → Dead*: Triggered by exceeding the timeout.
   - *Dead → Alive*: Triggered by receiving an updated *alive* status.

---

#### Design Considerations
1. **Fan-out Strategy:**
   - Each node gossips to a limited number of randomly selected nodes that are node DEAD (default: **2 nodes**). [ Take note the minimum is also two, the choice here depends on how many failures the system is allowed to have.]
   - This minimizes network overhead while ensuring state propagation.

2. **Consistency and Fault Tolerance:**
   - **Consistency:** Nodes maintain synchronized views using gossip. Conflicting updates are resolved by timestamps.
   - **Fault Tolerance:** Nodes recover from temporary failures without permanent exclusion unless explicitly dead.

3. **Scalability:**
   - The gossip mechanism scales with the cluster size, maintaining efficient communication through randomness and deduplication.

4. **Membership Management:**
   - Nodes are dynamically added or removed from the consistent hashing ring based on their status updates.

---

This section provides an overview of key logic and design decisions in the gossip protocol's implementation, focusing on fault detection, state merging, and scalability. Further details can be found in the protocol-specific documentation or codebase.

---

## **Summary of Changes**

### **Core Functionality**
- **`GossipHandler`**:
  - Manages gossip communication for a node.
  - Periodically sends membership updates to a subset of nodes (configurable fan-out).
  - Deduplicates incoming messages to prevent redundant processing.

- **Membership Management**:
  - Tracks the status of nodes in the cluster (`Alive`, `Suspect`, `Dead`).
  - Updates node information based on received gossip messages.
  - Integrates with the consistent hashing ring for dynamic cluster updates.

### **Components**

#### **`gossip.go`**
- **Primary Responsibilities**:
  - Initiate and manage periodic gossip broadcasts.
  - Handle incoming gossip messages, deduplicating and merging membership data.
  - Ensure only live nodes are selected for gossip exchanges.
  - Integrate with consistent hashing to update node placements dynamically.

- **Key Functions**:
  - `Start(ctx context.Context, gossipFanOut int)`: Starts the periodic gossip process.
  - `gossip(ctx context.Context, gossipFanOut int)`: Selects random live nodes for gossip communication.
  - `createGossipMessage()`: Constructs a message with the node's membership view.
  - `HandleGossipMessage(ctx context.Context, req *pb.GossipMessage)`: Processes incoming gossip messages and propagates updates.

#### **`membership.go`**
- **Primary Responsibilities**:
  - Maintain a synchronized view of cluster membership.
  - Add or update nodes based on received gossip information.
  - Mark nodes as `Suspect` or `Dead` based on timeouts and status changes.

- **Key Functions**:
  - `NewMembership(currentNodeInformation *types.Node)`: Initializes membership with the current node.
  - `AddOrUpdateNode(node *types.Node, chr *chr.ConsistentHashingRing)`: Handles node addition and updates, ensuring data consistency and ring updates.

---

## **Testing**
**TEST CASES CAN NOT BE RUN CONCURRENTLY SINCE I USED THE SAME NODE SET UP FUNCTION (SAME NODE NAMES ETC), TEST CASES ALSO NEED TO BE RUN INDIVIDUALLY SINCE A TIME OUT MIGHT OCCUR.**

### **Test Cases**
- TestGossipProtocolIntegration: Test the basic operation of the protocol under no fault. 
- TestAddNodesToStableSystem: Test adding a new node to the system, including a test for updating ring status. 
- TestRemoveUnresponsiveNode: This test marks an unresponsive node as SUSPECT, sets it as DEAD after a timeout, and removes it from the ring.
- TestDeadNodeRecovery: Test a node that is crashing intermittently and recovering. The node should be marked as DEAD and then later on as Alive and part of the membership.  

### **Test Coverage**
- Unit tests validate the following scenarios:
  - Gossip message creation and deduplication.
  - Correct node selection for gossiping.
  - Membership updates upon receiving valid gossip messages.
  - Marking nodes as `Suspect` or `Dead` based on timeouts.
  - Updating Ring Status based on membership consensus from Gossip Protocol. 

### **Tested Files**
1. **`gossip_test.go`**:
   - Verifies message propagation logic, deduplication, and fan-out correctness.
   - Asserts integration with consistent hashing for node updates.

---

## **Configuration**
- **Gossip Interval**: Configurable, Tested with `3 seconds` (can be adjusted for production deployments).
- **Gossip Fan-Out**: Configurable via `Start()` method.
- **Timeout for Suspect to Dead Transition**: Currently tested `5 seconds` (modifiable as required).

---


