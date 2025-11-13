"""Paxos node implementing proposer and acceptor roles (NO learners)."""

import json  # For serializing/deserializing state to/from disk
import os  # For file system operations (checking file existence)
import sys  # For command-line argument parsing
import threading  # For concurrent operations and lock-based synchronization
import time  # For delays in test scenarios
from multiprocessing.connection import Client  # For establishing RPC connections
import rpc_tools  # Custom RPC utilities for remote procedure calls

# Configuration - must match client.py exactly for proper communication
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),  # Node 1: IP address and port
    2: ('10.128.0.4', 17002),  # Node 2: IP address and port
    3: ('10.128.0.6', 17003),  # Node 3: IP address and port
}
AUTHKEY = b'paxos_lab_secret'  # Shared secret for authenticated connections
MAJORITY = len(PEERS_CONFIG) // 2 + 1  # 2 out of 3 nodes required for consensus


class PaxosNode:
    """Paxos node acting as both proposer and acceptor."""

    def __init__(self, node_id):
        # Node identity and network configuration
        self.node_id = node_id  # Unique identifier for this node (1, 2, or 3)
        self.address = PEERS_CONFIG[node_id]  # This node's (IP, port) tuple
        self.peers = {}  # Dictionary of connected peer nodes {peer_id: RPCProxy}
        
        # Thread-safe lock for protecting shared state from race conditions
        self.lock = threading.Lock()

        # File paths for persistent storage
        self.storage_file = f"CISC5597_node_{node_id}.txt"  # Stores the chosen value
        self.state_file = f"CISC5597_node_{node_id}_state.json"  # Stores Paxos state

        # Acceptor state (MUST be persistent per Paxos safety requirements)
        self.promised_id = (-1, -1)      # Highest proposal ID this acceptor promised to
        self.accepted_id = (-1, -1)      # Highest proposal ID this acceptor accepted
        self.accepted_value = None       # Value corresponding to accepted_id

        # Proposer state (persistent to avoid reusing proposal IDs after crash)
        self.proposal_counter = 0        # Counter for generating unique proposal IDs

        # Current file value (what has been written to disk)
        self.file_value = None

        # Load any previously saved state from disk
        self._load_state()
        print(f"Node {node_id} initialized. File value: {self.file_value}")

    # ========================================================================
    # Persistence
    # ========================================================================

    def _load_state(self):
        """Load persistent state from disk."""
        # Load the chosen value from the storage file
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    data = f.read()
                # Set file_value to the data, or None if file is empty
                self.file_value = data if data else None
            except Exception as e:
                print(f"[ERROR] Failed to read storage file: {e}")
        else:
            # Create an empty storage file if it doesn't exist
            open(self.storage_file, 'w').close()

        # Load acceptor and proposer state from the JSON state file
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                
                # Restore acceptor state (convert lists back to tuples)
                self.promised_id = tuple(state.get("promised_id", [-1, -1]))
                self.accepted_id = tuple(state.get("accepted_id", [-1, -1]))
                self.accepted_value = state.get("accepted_value")
                
                # Restore proposer counter (critical to avoid reusing proposal IDs)
                self.proposal_counter = state.get("proposal_counter", 0)
            except Exception as e:
                print(f"[ERROR] Failed to read state file: {e}")
        else:
            # Create initial state file if it doesn't exist
            self._persist_state()

    def _persist_state(self):
        """Persist acceptor and proposer state to disk."""
        # Package all state that needs to survive crashes
        state = {
            "promised_id": list(self.promised_id),  # Convert tuple to list for JSON
            "accepted_id": list(self.accepted_id),
            "accepted_value": self.accepted_value,
            "proposal_counter": self.proposal_counter,  # Prevent ID reuse after crash
        }
        
        try:
            # Write state atomically to the JSON file
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            print(f"[ERROR] Failed to persist state: {e}")

    def _write_to_file(self, value):
        """Write value to file after successful consensus."""
        try:
            # Write the chosen value to the storage file
            with open(self.storage_file, 'w') as f:
                f.write(str(value) if value else "")
            
            # Update in-memory copy to match disk
            self.file_value = value
        except Exception as e:
            print(f"[ERROR] Failed to write to file: {e}")

    # ========================================================================
    # Networking
    # ========================================================================

    def connect_to_peers(self):
        """Connect to all other nodes in the cluster."""
        print(f"Node {self.node_id}: Connecting to peers...")
        
        # Iterate through all configured peers
        for peer_id, addr in PEERS_CONFIG.items():
            # Don't try to connect to ourselves
            if peer_id == self.node_id:
                continue
            
            # Keep retrying until connection succeeds
            while peer_id not in self.peers:
                try:
                    # Establish authenticated connection to peer
                    conn = Client(addr, authkey=AUTHKEY)
                    
                    # Create RPC proxy for remote method invocation
                    self.peers[peer_id] = rpc_tools.RPCProxy(conn)
                    print(f"Node {self.node_id}: Connected to peer {peer_id}")
                except Exception:
                    # Connection failed, wait before retrying
                    time.sleep(3)
        
        print(f"--- Node {self.node_id}: All peers connected! ---\n")

    def _broadcast_rpc(self, method_name, *args):
        """Broadcast RPC call to all peers and self."""
        responses = {}
        
        # Call the method on all remote peer nodes
        for peer_id, proxy in self.peers.items():
            try:
                # Invoke remote method using RPC proxy
                responses[peer_id] = getattr(proxy, method_name)(*args)
            except Exception as e:
                # Log error and mark response as failed
                print(f"[ERROR] RPC {method_name} to {peer_id} failed: {e}")
                responses[peer_id] = None
        
        # Call the method locally on this node (no network overhead)
        try:
            responses[self.node_id] = getattr(self, method_name)(*args)
        except Exception as e:
            print(f"[ERROR] Local {method_name} failed: {e}")
            responses[self.node_id] = None
        
        return responses

    def _broadcast_rpc_with_delays(self, method_name, delays_dict, *args):
        """Broadcast RPC call with per-node delays to simulate network latency.
        
        Args:
            method_name: Name of the RPC method to call
            delays_dict: Dict mapping node_id to delay in seconds {node_id: delay}
            *args: Arguments to pass to the method
        """
        responses = {}
        threads = []
        
        def delayed_call(node_id, delay):
            """Helper to call RPC after delay."""
            if delay > 0:
                time.sleep(delay)
            
            try:
                if node_id == self.node_id:
                    # Local call
                    responses[node_id] = getattr(self, method_name)(*args)
                else:
                    # Remote call
                    proxy = self.peers[node_id]
                    responses[node_id] = getattr(proxy, method_name)(*args)
            except Exception as e:
                print(f"[ERROR] RPC {method_name} to {node_id} failed: {e}")
                responses[node_id] = None
        
        # Create threads for each node with their respective delays
        all_node_ids = list(self.peers.keys()) + [self.node_id]
        for node_id in all_node_ids:
            delay = delays_dict.get(node_id, 0.0)  # Default to no delay
            t = threading.Thread(target=delayed_call, args=(node_id, delay))
            threads.append(t)
            t.start()
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        return responses

    # ========================================================================
    # Acceptor RPCs (Phase 1 & 2)
    # ========================================================================

    def rpc_prepare(self, proposal_id):
        """Phase 1: Acceptor responds to PREPARE request."""
        # Acquire lock to ensure atomic read-modify-write operation
        with self.lock:
            # Only promise if the proposal_id is strictly higher than any previous promise
            if proposal_id > self.promised_id:
                # Update promise to this new, higher proposal ID
                self.promised_id = proposal_id
                
                # Persist promise immediately (critical for safety)
                self._persist_state()
                print(f"  [ACCEPTOR {self.node_id}] PROMISE: {proposal_id}")
                
                # Return promise along with any previously accepted value
                # (proposer must adopt the highest accepted value it sees)
                return {
                    "status": "promise",
                    "promised_id": self.promised_id,
                    "accepted_id": self.accepted_id,      # For value adoption check
                    "accepted_value": self.accepted_value  # Value to potentially adopt
                }
            else:
                # Reject because we already promised to a higher proposal
                print(f"  [ACCEPTOR {self.node_id}] REJECT: {proposal_id} <= {self.promised_id}")
                return {
                    "status": "reject",
                    "promised_id": self.promised_id  # Tell proposer what we're bound to
                }

    def rpc_accept(self, proposal_id, value):
        """Phase 2: Acceptor responds to ACCEPT request."""
        # Acquire lock to ensure atomic read-modify-write operation
        with self.lock:
            # Accept if proposal_id >= promised_id (must honor our promise)
            if proposal_id >= self.promised_id:
                # Update acceptor state to reflect this acceptance
                self.promised_id = proposal_id
                self.accepted_id = proposal_id
                self.accepted_value = value
                
                # Persist acceptance immediately (critical for safety)
                self._persist_state()
                
                # DO NOT write to file yet - wait for consensus notification
                print(f"  [ACCEPTOR {self.node_id}] ACCEPTED: {proposal_id} = '{value}'")
                
                return {
                    "status": "accepted",
                    "promised_id": self.promised_id
                }
            else:
                # Reject because proposal_id is lower than our promise
                print(f"  [ACCEPTOR {self.node_id}] REJECT: {proposal_id} < {self.promised_id}")
                return {
                    "status": "reject",
                    "promised_id": self.promised_id  # Tell proposer what we're bound to
                }

    # ========================================================================
    # Proposer RPC (Main Entry Point)
    # ========================================================================

    def rpc_submit_value(self, value, scenario_delay=0, phase2_accept_delays=None, inter_phase_delay=0):
        """Proposer entry point: propose a value using Paxos consensus.
        
        Args:
            value: The value this proposer wants to propose
            scenario_delay: Initial delay before starting (for test scenarios)
            phase2_accept_delays: Dict {node_id: delay} for simulating network delays
            inter_phase_delay: Delay between Phase 1 and Phase 2 (for scenario 4 timing)
        """
        # Log the proposal request from the client
        print(f"\n[PROPOSER {self.node_id}] Submit request: '{value}' (delay {scenario_delay}s)")
        
        # Honor the test scenario delay before starting consensus
        time.sleep(scenario_delay)

        # Attempt one round of Paxos consensus
        result = self._try_consensus(value, phase2_accept_delays, inter_phase_delay)
        
        # Return the result (success or failure)
        return result

    def _try_consensus(self, value, phase2_accept_delays=None, inter_phase_delay=0):
        """Attempt one round of Paxos consensus.
        
        Args:
            value: The value this proposer wants to propose
            phase2_accept_delays: Optional dict {node_id: delay} for network simulation
            inter_phase_delay: Optional delay between Phase 1 and Phase 2 (for scenario 4)
        """
        # ===== Generate Unique Proposal ID =====
        with self.lock:  # Lock to ensure atomic counter increment
            self.proposal_counter += 1  # Increment to get next proposal number
            proposal_id = (self.proposal_counter, self.node_id)  # (counter, node_id) ensures uniqueness
        
        # ===== PHASE 1: PREPARE =====
        print(f"[PROPOSER {self.node_id}] Phase 1 PREPARE: {proposal_id}")
        
        # Broadcast PREPARE to all acceptors asking them to promise
        responses = self._broadcast_rpc('rpc_prepare', proposal_id)
        
        # Count how many acceptors sent back promises (not rejections)
        promises = [r for r in responses.values() if r and r.get('status') == 'promise']
        
        # Check if we got majority - if not, this proposal fails
        if len(promises) < MAJORITY:
            print(f"[PROPOSER {self.node_id}] Phase 1 FAILED: {len(promises)}/{MAJORITY} promises")
            return {"status": "fail", "reason": "prepare_rejected", "proposal_id": proposal_id}
        
        # We got majority! Phase 1 succeeded
        print(f"[PROPOSER {self.node_id}] Phase 1 SUCCESS: {len(promises)} promises")
        
        # Optional delay between Phase 1 and Phase 2 (for scenario 4 timing)
        if inter_phase_delay > 0:
            time.sleep(inter_phase_delay)
        
        # ===== VALUE ADOPTION (Paxos Safety) =====
        value_to_propose = value  # Start with our originally requested value
        highest_accepted_id = (-1, -1)  # Track the highest proposal ID we've seen
        
        # Look through all promise responses for previously accepted values
        for res in promises:
            # If this acceptor previously accepted a value with higher ID, we must adopt it
            if res['accepted_id'] > highest_accepted_id:
                highest_accepted_id = res['accepted_id']  # Update highest ID seen
                value_to_propose = res['accepted_value']  # MUST use this value (Paxos safety!)
        
        # If we're adopting a different value than originally requested, log it
        if value_to_propose != value:
            print(f"[PROPOSER {self.node_id}] Adopting value '{value_to_propose}' from {highest_accepted_id}")
        
        # ===== PHASE 2: ACCEPT =====
        print(f"[PROPOSER {self.node_id}] Phase 2 ACCEPT: {proposal_id} = '{value_to_propose}'")
        
        # Broadcast ACCEPT message with our chosen value
        if phase2_accept_delays:
            # Use delayed broadcast if network simulation is enabled
            responses = self._broadcast_rpc_with_delays('rpc_accept', phase2_accept_delays, proposal_id, value_to_propose)
        else:
            # Normal broadcast - all nodes get message immediately
            responses = self._broadcast_rpc('rpc_accept', proposal_id, value_to_propose)
        
        # Count how many acceptors accepted our proposal
        accepts = [r for r in responses.values() if r and r.get('status') == 'accepted']
        
        # Check if we achieved majority acceptance - if not, this proposal fails
        if len(accepts) < MAJORITY:
            print(f"[PROPOSER {self.node_id}] Phase 2 FAILED: {len(accepts)}/{MAJORITY} accepts")
            return {"status": "fail", "reason": "accept_rejected", "proposal_id": proposal_id}
        
        # ===== CONSENSUS ACHIEVED =====
        # We got majority accepts - the value is now chosen!
        print(f"[PROPOSER {self.node_id}] Phase 2 SUCCESS: '{value_to_propose}' is CHOSEN")
        
        # Notify all nodes that consensus is reached so they can write to file
        print(f"[PROPOSER {self.node_id}] Notifying all nodes that value is chosen...")
        self._broadcast_rpc('rpc_value_chosen', proposal_id, value_to_propose)
        
        # Return success with the chosen value
        return {"status": "success", "value": value_to_propose}

    # ========================================================================
    # Consensus Notification (Called by proposer when value is chosen)
    # ========================================================================

    def rpc_value_chosen(self, proposal_id, value):
        """Notification that a value has been chosen by consensus.
        
        This is called by the proposer after achieving majority acceptance.
        Now it's safe to write the value to the application's stable storage.
        """
        with self.lock:
            # Only write if we haven't written yet
            if self.file_value is None:
                self._write_to_file(value)
                print(f"  [NODE {self.node_id}] VALUE CHOSEN: '{value}' (wrote to file)")
                return {"status": "written"}
            else:
                print(f"  [NODE {self.node_id}] VALUE CHOSEN: '{value}' (already written)")
                return {"status": "already_written"}

    # ========================================================================
    # Utility RPCs
    # ========================================================================

    def rpc_get_file_content(self):
        """Get the current file value."""
        # Thread-safe read of the current chosen value
        with self.lock:
            return self.file_value

    def rpc_reset_state(self):
        """Reset node to initial state (for testing)."""
        # Thread-safe reset of all state
        with self.lock:
            print(f"[RESET] Node {self.node_id} resetting...")
            
            # Reset all state variables to initial values
            self.promised_id = (-1, -1)
            self.accepted_id = (-1, -1)
            self.accepted_value = None
            self.proposal_counter = 0
            self.file_value = None
            
            try:
                # Clear the storage file
                open(self.storage_file, 'w').close()
                
                # Persist the reset state to disk
                self._persist_state()
                print(f"[RESET] Node {self.node_id} complete")
                return {"status": "success"}
            except Exception as e:
                # Report any errors during reset
                print(f"[RESET ERROR] {e}")
                return {"status": "error", "message": str(e)}


# ============================================================================
# Main
# ============================================================================

def main():
    # Validate command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 node.py <node_id>")
        print("Example: python3 node.py 1")
        sys.exit(1)

    try:
        # Parse node_id from command line
        node_id = int(sys.argv[1])
        
        # Verify it's a valid node ID from our configuration
        if node_id not in PEERS_CONFIG:
            raise ValueError
    except ValueError:
        print(f"Invalid node_id. Must be one of {list(PEERS_CONFIG.keys())}")
        sys.exit(1)

    # Create Paxos node instance
    node = PaxosNode(node_id)

    # Connect to peers in background thread (non-blocking)
    peer_thread = threading.Thread(target=node.connect_to_peers)
    peer_thread.daemon = True  # Allow main to exit even if thread is running
    peer_thread.start()

    # Register all RPC methods that can be called remotely
    handler = rpc_tools.RPCHandler()
    handler.register_function(node.rpc_prepare)            # Phase 1 acceptor handler
    handler.register_function(node.rpc_accept)             # Phase 2 acceptor handler
    handler.register_function(node.rpc_value_chosen)       # Consensus notification handler
    handler.register_function(node.rpc_submit_value)       # Proposer entry point
    handler.register_function(node.rpc_get_file_content)   # Query current value
    handler.register_function(node.rpc_reset_state)        # Reset for testing

    # Start RPC server (blocks forever, handling incoming requests)
    try:
        rpc_tools.rpc_server(handler, node.address, authkey=AUTHKEY)
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Server error: {e}")


if __name__ == "__main__":
    main()