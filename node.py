"""Paxos node implementing proposer and acceptor roles (NO learners)."""

import json
import os
import sys
import threading
import time
import random
from multiprocessing.connection import Client
import rpc_tools

# Configuration - must match client.py
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),
    2: ('10.128.0.4', 17002),
    3: ('10.128.0.6', 17003),
}
AUTHKEY = b'paxos_lab_secret'
MAJORITY = len(PEERS_CONFIG) // 2 + 1  # 2 out of 3 nodes

# Retry configuration for livelock prevention (Bonus-2)
MAX_RETRIES = 10       # Maximum number of retry attempts
MIN_BACKOFF = 0.1      # Minimum backoff time (seconds)
MAX_BACKOFF = 5.0      # Maximum backoff time (seconds)


class PaxosNode:
    """Paxos node acting as both proposer and acceptor."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.address = PEERS_CONFIG[node_id]
        self.peers = {}  # Connected peer nodes {peer_id: proxy}
        self.lock = threading.Lock()  # Protects shared state access

        # File paths for persistence
        self.storage_file = f"CISC5597_node_{node_id}.txt"  # Where value is stored
        self.state_file = f"CISC5597_node_{node_id}_state.json"  # Paxos state

        # Acceptor state (must be persistent per Paxos requirements)
        self.promised_id = (-1, -1)      # Highest proposal ID promised to
        self.accepted_id = (-1, -1)      # Highest proposal ID accepted
        self.accepted_value = None       # Value corresponding to accepted_id

        # Proposer state (persistent to avoid reusing proposal IDs after crash)
        self.proposal_counter = 0        # Counter for generating unique proposal IDs

        # Current file value (written when value is accepted)
        self.file_value = None

        self._load_state()  # Restore state from disk if exists
        print(f"Node {node_id} initialized. File value: {self.file_value}")

    # ========================================================================
    # Persistence
    # ========================================================================

    def _load_state(self):
        """Load persistent state from disk."""
        # Load file value from storage file
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    data = f.read()
                self.file_value = data if data else None
            except Exception as e:
                print(f"[ERROR] Failed to read storage file: {e}")
        else:
            open(self.storage_file, 'w').close()  # Create empty file

        # Load acceptor state from JSON file
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                # Restore acceptor state
                self.promised_id = tuple(state.get("promised_id", [-1, -1]))
                self.accepted_id = tuple(state.get("accepted_id", [-1, -1]))
                self.accepted_value = state.get("accepted_value")
                # Restore proposer counter (important to avoid reusing IDs)
                self.proposal_counter = state.get("proposal_counter", 0)
            except Exception as e:
                print(f"[ERROR] Failed to read state file: {e}")
        else:
            self._persist_state()  # Create initial state file

    def _persist_state(self):
        """Persist acceptor and proposer state to disk."""
        state = {
            "promised_id": list(self.promised_id),
            "accepted_id": list(self.accepted_id),
            "accepted_value": self.accepted_value,
            "proposal_counter": self.proposal_counter,  # Persisted to avoid ID reuse
        }
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            print(f"[ERROR] Failed to persist state: {e}")

    def _write_to_file(self, value):
        """Write value to file after successful consensus."""
        try:
            with open(self.storage_file, 'w') as f:
                f.write(str(value) if value else "")
            self.file_value = value  # Update in-memory copy
        except Exception as e:
            print(f"[ERROR] Failed to write to file: {e}")

    # ========================================================================
    # Networking
    # ========================================================================

    def connect_to_peers(self):
        """Connect to all other nodes in the cluster."""
        print(f"Node {self.node_id}: Connecting to peers...")
        for peer_id, addr in PEERS_CONFIG.items():
            if peer_id == self.node_id:
                continue  # Don't connect to self
            
            # Keep trying to connect until successful
            while peer_id not in self.peers:
                try:
                    conn = Client(addr, authkey=AUTHKEY)
                    self.peers[peer_id] = rpc_tools.RPCProxy(conn)
                    print(f"Node {self.node_id}: Connected to peer {peer_id}")
                except Exception:
                    time.sleep(3)  # Wait before retrying
        
        print(f"--- Node {self.node_id}: All peers connected! ---\n")

    def _broadcast_rpc(self, method_name, *args):
        """Broadcast RPC call to all peers and self."""
        responses = {}
        
        # Call all peer nodes
        for peer_id, proxy in self.peers.items():
            try:
                responses[peer_id] = getattr(proxy, method_name)(*args)
            except Exception as e:
                print(f"[ERROR] RPC {method_name} to {peer_id} failed: {e}")
                responses[peer_id] = None  # Mark as failed
        
        # Call self locally (no network needed)
        try:
            responses[self.node_id] = getattr(self, method_name)(*args)
        except Exception as e:
            print(f"[ERROR] Local {method_name} failed: {e}")
            responses[self.node_id] = None
        
        return responses

    # ========================================================================
    # Acceptor RPCs (Phase 1 & 2)
    # ========================================================================

    def rpc_prepare(self, proposal_id):
        """Phase 1: Acceptor responds to PREPARE request."""
        with self.lock:  # Ensure atomic state check and update
            # Only promise if proposal_id is higher than any previously promised
            if proposal_id > self.promised_id:
                self.promised_id = proposal_id  # Update promise
                self._persist_state()  # Persist promise to survive crashes
                print(f"  [ACCEPTOR {self.node_id}] PROMISE: {proposal_id}")
                
                # Return promise with any previously accepted value
                return {
                    "status": "promise",
                    "promised_id": self.promised_id,
                    "accepted_id": self.accepted_id,      # Return for value adoption
                    "accepted_value": self.accepted_value  # Return for value adoption
                }
            else:
                # Reject because we already promised to higher proposal
                print(f"  [ACCEPTOR {self.node_id}] REJECT: {proposal_id} <= {self.promised_id}")
                return {
                    "status": "reject",
                    "promised_id": self.promised_id  # Tell proposer what we promised
                }

    def rpc_accept(self, proposal_id, value):
        """Phase 2: Acceptor responds to ACCEPT request."""
        with self.lock:  # Ensure atomic state check and update
            # Accept if proposal_id >= promised_id
            if proposal_id >= self.promised_id:
                # Update acceptor state
                self.promised_id = proposal_id
                self.accepted_id = proposal_id
                self.accepted_value = value
                self._persist_state()  # Persist acceptance to survive crashes
                
                # Write to file immediately upon accepting (no learner phase)
                if self.file_value is None:
                    self._write_to_file(value)
                    print(f"  [ACCEPTOR {self.node_id}] ACCEPTED: {proposal_id} = '{value}' (wrote to file)")
                else:
                    print(f"  [ACCEPTOR {self.node_id}] ACCEPTED: {proposal_id} = '{value}'")
                
                return {
                    "status": "accepted",
                    "promised_id": self.promised_id
                }
            else:
                # Reject because proposal_id < promised_id
                print(f"  [ACCEPTOR {self.node_id}] REJECT: {proposal_id} < {self.promised_id}")
                return {
                    "status": "reject",
                    "promised_id": self.promised_id
                }

    # ========================================================================
    # Proposer RPC (Main Entry Point)
    # ========================================================================

    def rpc_submit_value(self, value, scenario_delay=0, enable_retry=False):
        """
        Proposer entry point: propose a value using Paxos consensus.
        
        Args:
            value: Value to propose
            scenario_delay: Initial delay (for test scenarios)
            enable_retry: If True, retry on failure with exponential backoff (Bonus-2)
        """
        print(f"\n[PROPOSER {self.node_id}] Submit request: '{value}' (delay {scenario_delay}s)")
        time.sleep(scenario_delay)  # Honor test scenario delay

        # Determine max attempts based on retry flag
        max_attempts = MAX_RETRIES if enable_retry else 1
        
        for attempt in range(max_attempts):
            # Apply jitter and backoff for livelock prevention
            if attempt == 0:
                # Initial jitter to break symmetry
                jitter = random.uniform(0, 0.1)
                time.sleep(jitter)
                if enable_retry:
                    print(f"[PROPOSER {self.node_id}] Attempt {attempt + 1}/{max_attempts} (jitter: {jitter:.3f}s)")
            else:
                # Exponential backoff with randomization
                backoff = min(MIN_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
                backoff *= random.uniform(0.5, 1.5)  # Add randomness
                print(f"[PROPOSER {self.node_id}] Attempt {attempt + 1}/{max_attempts} (backoff: {backoff:.3f}s)")
                time.sleep(backoff)

            # Try to achieve consensus
            result = self._try_consensus(value, enable_retry)
            
            if result["status"] == "success":
                if enable_retry:
                    result["attempts"] = attempt + 1  # Include attempt count
                return result
            
            # Retry if enabled and not last attempt
            if enable_retry and attempt < max_attempts - 1:
                print(f"[PROPOSER {self.node_id}] Will retry...")
            else:
                return result  # Give up
        
        return {"status": "fail", "reason": "max_retries_exceeded"}

    def _try_consensus(self, value, enable_retry):
        """Attempt one round of Paxos consensus."""
        # Generate new unique proposal ID: (counter, node_id)
        with self.lock:
            self.proposal_counter += 1
            proposal_id = (self.proposal_counter, self.node_id)
        
        print(f"[PROPOSER {self.node_id}] Phase 1 PREPARE: {proposal_id}")
        
        # Phase 1: PREPARE - ask acceptors to promise
        responses = self._broadcast_rpc('rpc_prepare', proposal_id)
        promises = [r for r in responses.values() if r and r.get('status') == 'promise']
        
        # Check if we got majority of promises
        if len(promises) < MAJORITY:
            print(f"[PROPOSER {self.node_id}] Phase 1 FAILED: {len(promises)}/{MAJORITY} promises")
            return {"status": "fail", "reason": "prepare_rejected", "proposal_id": proposal_id}
        
        print(f"[PROPOSER {self.node_id}] Phase 1 SUCCESS: {len(promises)} promises")
        
        # Adopt highest accepted value if any (for safety)
        value_to_propose = value
        highest_accepted_id = (-1, -1)
        for res in promises:
            # Check if this acceptor has accepted a value
            if res['accepted_id'] > highest_accepted_id:
                highest_accepted_id = res['accepted_id']
                value_to_propose = res['accepted_value']  # Must propose this value
        
        # Log if we're adopting a different value
        if value_to_propose != value:
            print(f"[PROPOSER {self.node_id}] Adopting value '{value_to_propose}' from {highest_accepted_id}")
        
        # Phase 2: ACCEPT - ask acceptors to accept value
        print(f"[PROPOSER {self.node_id}] Phase 2 ACCEPT: {proposal_id} = '{value_to_propose}'")
        responses = self._broadcast_rpc('rpc_accept', proposal_id, value_to_propose)
        accepts = [r for r in responses.values() if r and r.get('status') == 'accepted']
        
        # Check if we got majority of accepts
        if len(accepts) < MAJORITY:
            print(f"[PROPOSER {self.node_id}] Phase 2 FAILED: {len(accepts)}/{MAJORITY} accepts")
            return {"status": "fail", "reason": "accept_rejected", "proposal_id": proposal_id}
        
        # Success! Value is chosen by consensus
        print(f"[PROPOSER {self.node_id}] Phase 2 SUCCESS: '{value_to_propose}' is CHOSEN")
        
        return {"status": "success", "value": value_to_propose}

    # ========================================================================
    # Utility RPCs
    # ========================================================================

    def rpc_get_file_content(self):
        """Get the current file value."""
        with self.lock:
            return self.file_value

    def rpc_reset_state(self):
        """Reset node to initial state (for testing)."""
        with self.lock:
            print(f"[RESET] Node {self.node_id} resetting...")
            # Reset all state to initial values
            self.promised_id = (-1, -1)
            self.accepted_id = (-1, -1)
            self.accepted_value = None
            self.proposal_counter = 0
            self.file_value = None
            
            try:
                # Clear files
                open(self.storage_file, 'w').close()
                self._persist_state()
                print(f"[RESET] Node {self.node_id} complete")
                return {"status": "success"}
            except Exception as e:
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
        node_id = int(sys.argv[1])
        if node_id not in PEERS_CONFIG:
            raise ValueError
    except ValueError:
        print(f"Invalid node_id. Must be one of {list(PEERS_CONFIG.keys())}")
        sys.exit(1)

    # Create Paxos node
    node = PaxosNode(node_id)

    # Connect to peers in background thread
    peer_thread = threading.Thread(target=node.connect_to_peers)
    peer_thread.daemon = True  # Allow main to exit even if thread running
    peer_thread.start()

    # Register RPC handlers
    handler = rpc_tools.RPCHandler()
    handler.register_function(node.rpc_prepare)            # Phase 1 handler
    handler.register_function(node.rpc_accept)             # Phase 2 handler
    handler.register_function(node.rpc_submit_value)       # Proposer entry point
    handler.register_function(node.rpc_get_file_content)   # For checking state
    handler.register_function(node.rpc_reset_state)        # For testing

    # Start RPC server (blocks forever)
    try:
        rpc_tools.rpc_server(handler, node.address, authkey=AUTHKEY)
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Server error: {e}")


if __name__ == "__main__":
    main()