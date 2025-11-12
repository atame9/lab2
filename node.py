"""Combined proposer/acceptor node for the Paxos lab."""

import json  # Handles persistence of acceptor state
import os  # Provides filesystem utilities for persistence checks
import sys  # Access to command-line arguments for node ID selection
import threading  # Provides threads for background peer connections
import time  # Supplies sleep for retry/backoff windows
import random  # Generates randomized delays to simulate timing races
from multiprocessing.connection import Client  # Establishes authenticated IPC channels
import rpc_tools  # Imports shared RPC proxy/server helpers

# --- CONFIGURATION ---
# !! IMPORTANT !!
# Update these IPs to your Google Cloud node's internal or external IPs.
# For testing on one machine, you can use 'localhost'.
PEERS_CONFIG = {  # Static mapping of node identifiers to network endpoints
    1: ('10.128.0.3', 17001),  # Node 1 bound address
    2: ('10.128.0.4', 17002),  # Node 2 bound address
    3: ('10.128.0.6', 17003),  # Node 3 bound address
}
AUTHKEY = b'paxos_lab_secret'  # Shared authentication token for all IPC connections
MAJORITY = 2  # Majority threshold for a three-node cluster [cite: 6]
# --- END CONFIGURATION ---


class PaxosNode:  # Encapsulates proposer, acceptor, and learner behavior for one node
    """
    Implements a Paxos node that is both a Proposer and an Acceptor. [cite: 13]
    Based on the "Single Decree Paxos: Protocol" slide (image_82e57d.png).
    """

    def __init__(self, node_id):  # Initialize all role-specific state for this node
        self.node_id = node_id  # Numeric identifier for this node instance
        self.address = PEERS_CONFIG[node_id]  # Lookup local (host, port) tuple
        self.peers = {}  # Holds RPC proxies to each connected peer {peer_id: proxy}
        self.lock = threading.Lock()  # Serializes access to shared Paxos state

        # File to store the chosen value [cite: 10, 15]
        self.storage_file = f"CISC5597_node_{self.node_id}.txt"  # Per-node storage path
        # File to persist acceptor metadata for crash recovery
        self.state_file = f"CISC5597_node_{self.node_id}_state.json"

        # --- Paxos Acceptor State ---
        # (Must be persistent, per slide)
        self.promised_id = (-1, -1)     # Highest proposal ID promised during Prepare
        self.accepted_id = (-1, -1)     # Highest proposal ID accepted in Phase 2
        self.accepted_value = None      # Value that corresponds to accepted_id

        # --- Paxos Proposer State ---
        self.proposal_counter = 0       # Local counter for generating unique proposal IDs

        # Final chosen value
        self.chosen_value = None  # Learner-visible committed value
        self._load_persistent_state()  # Restore previously persisted state, if any
        print(f"Node {self.node_id} initialized.")  # Log initialization summary
        print(f"  > Acceptor state: promised_id={self.promised_id}, accepted_id={self.accepted_id}")  # Show acceptor state
        print(f"  > Current learned value: {self.chosen_value}")  # Display restored learner value
        print(f"  > Storage file: {self.storage_file}")  # Indicate backing file location

    def _write_to_file(self, value):  # Persist the current chosen value to disk
        """Helper to write the chosen value to the replicated file."""
        try:
            with open(self.storage_file, 'w') as f:  # Open file for overwrite
                f.write(str(value) if value is not None else "")  # Persist stringified value or blank placeholder
        except Exception as e:
            print(f"[ERROR] Node {self.node_id} failed to write to file: {e}")  # Report I/O failures

    def _persist_acceptor_state(self):  # Store acceptor metadata for crash recovery
        state_payload = {
            "promised_id": list(self.promised_id),
            "accepted_id": list(self.accepted_id),
            "accepted_value": self.accepted_value,
        }
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state_payload, f)
        except Exception as e:
            print(f"[ERROR] Node {self.node_id} failed to write acceptor state file: {e}")

    def _load_persistent_state(self):  # Restore learner and acceptor state from disk when available
        # Recover previously chosen value
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    data = f.read()
                self.chosen_value = data if data != "" else None
            except Exception as e:
                print(f"[ERROR] Node {self.node_id} failed to read storage file: {e}")
        else:
            # Ensure file exists for future writes
            try:
                with open(self.storage_file, 'w'):
                    pass
            except Exception as e:
                print(f"[ERROR] Node {self.node_id} failed to create storage file: {e}")

        # Recover acceptor state if previously persisted
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                promised_raw = state.get("promised_id", [-1, -1])
                accepted_raw = state.get("accepted_id", [-1, -1])
                self.promised_id = tuple(promised_raw) if promised_raw is not None else (-1, -1)
                self.accepted_id = tuple(accepted_raw) if accepted_raw is not None else (-1, -1)
                self.accepted_value = state.get("accepted_value")
            except Exception as e:
                print(f"[ERROR] Node {self.node_id} failed to read acceptor state file: {e}")
        else:
            # Persist default state so subsequent updates have a file to overwrite
            self._persist_acceptor_state()

    def connect_to_peers(self):  # Establish outbound connections to every other node
        """
        Attempts to connect to all other nodes in the PEERS_CONFIG.
        This runs in a separate thread. [cite: 9, 35]
        """
        print(f"Node {self.node_id}: Attempting to connect to peers...")  # Announce connection attempts
        for peer_id, addr in PEERS_CONFIG.items():  # Iterate over every configured node
            if peer_id == self.node_id:  # Skip self to avoid self-connection
                continue  # Move to the next peer entry

            while peer_id not in self.peers:  # Loop until connection succeeds
                try:
                    conn = Client(addr, authkey=AUTHKEY)  # Establish authenticated connection to peer
                    self.peers[peer_id] = rpc_tools.RPCProxy(conn)  # Retain proxy for future RPC calls
                    print(f"Node {self.node_id}: Connected to peer {peer_id} at {addr}")  # Confirm success
                except Exception:
                    time.sleep(3)  # Wait before retrying to reduce spam
        print(f"\n--- Node {self.node_id}: All peers connected! ---\n")  # Indicate readiness

    # --- Acceptor RPC Functions ---

    def rpc_prepare(self, proposal_id):  # Handle incoming Phase 1 prepare requests
        """
        Acceptor logic for Phase 1 (Prepare).
        Corresponds to step 3 on the protocol slide.
        """
        with self.lock:  # Guard state updates
            if proposal_id > self.promised_id:  # Only promise higher proposal IDs
                self.promised_id = proposal_id  # Record newest promise
                self._persist_acceptor_state()  # Persist promise so restart keeps safety guarantees
                print(f"  [ACCEPTOR {self.node_id}] PROMISE: New proposal {proposal_id}. Promising.")  # Log promise event
                # Return promise, and any previously accepted value
                return {
                    "status": "promise",  # Indicates success
                    "promised_id": self.promised_id,  # Report latest promise ID
                    "accepted_id": self.accepted_id,  # Share prior accepted proposal ID
                    "accepted_value": self.accepted_value  # Share prior accepted value
                }
            else:
                print(f"  [ACCEPTOR {self.node_id}] REJECT: Proposal {proposal_id} is <= promised {self.promised_id}.")  # Log rejection
                # Return rejection
                return {
                    "status": "reject",  # Signal failure to caller
                    "promised_id": self.promised_id  # Communicate current promise threshold
                }

    def rpc_accept(self, proposal_id, value):  # Handle incoming Phase 2 accept requests
        """
        Acceptor logic for Phase 2 (Accept).
        Corresponds to step 6 on the protocol slide.
        """
        with self.lock:  # Ensure atomic comparison and update
            if proposal_id >= self.promised_id:  # Accept if proposal matches/ exceeds promise
                self.promised_id = proposal_id  # Upgrade promise to accepted proposal
                self.accepted_id = proposal_id  # Persist accepted proposal ID
                self.accepted_value = value  # Persist associated value
                self._persist_acceptor_state()  # Persist acceptor state for crash recovery
                print(f"  [ACCEPTOR {self.node_id}] ACCEPTED: Proposal {proposal_id} with value '{value}'.")  # Log acceptance
                # Return accepted
                return {
                    "status": "accepted",  # Notify proposer of success
                    "promised_id": self.promised_id  # Share current promise floor
                }
            else:
                print(f"  [ACCEPTOR {self.node_id}] REJECT: Proposal {proposal_id} is < promised {self.promised_id}.")  # Explain rejection
                # Return rejection
                return {
                    "status": "reject",  # Notify proposer of failure
                    "promised_id": self.promised_id  # Provide latest promise threshold
                }

    def rpc_get_file_content(self):  # Allow clients to read the chosen value
        """Client RPC to check the final chosen value."""
        with self.lock:  # Ensure consistent read of learner state
            return self.chosen_value  # Return locally learned value (could be None)

    def rpc_reset_state(self):  # Reset node to initial state for testing
        """Reset this node to initial state (for testing scenarios)."""
        with self.lock:
            print(f"[RESET] Node {self.node_id} resetting state...")
            
            # Reset Paxos state
            self.promised_id = (-1, -1)
            self.accepted_id = (-1, -1)
            self.accepted_value = None
            self.proposal_counter = 0
            self.chosen_value = None
            
            # Clear storage files
            try:
                with open(self.storage_file, 'w') as f:
                    f.write("")
                self._persist_acceptor_state()
                print(f"[RESET] Node {self.node_id} state cleared successfully")
                return {"status": "success"}
            except Exception as e:
                print(f"[RESET ERROR] Node {self.node_id}: {e}")
                return {"status": "error", "message": str(e)}

    # --- Proposer RPC Function (Client-facing) ---

    def rpc_submit_value(self, value, scenario_delay=0):  # Client-facing entry point for proposals
        """
        Client submits a value. This node becomes a Proposer. [cite: 16, 17]
        This implements the Proposer logic (steps 1, 2, 4, 5, 7) with retry logic.
        
        (Bonus-2) Implements randomized restart for livelock prevention:
        - If proposal fails, retry with exponential backoff
        - Random jitter breaks symmetry between competing proposers
        """

        # Add a random delay to simulate network/processing differences [cite: 28, 30]
        print(f"\n[PROPOSER {self.node_id}] Received submit request for '{value}' with delay {scenario_delay}s")
        time.sleep(scenario_delay)  # Honor scripted scenario delay

        # (Bonus-2) Retry loop with randomized backoff for livelock prevention
        MAX_RETRIES = 10
        MIN_BACKOFF = 0.1
        MAX_BACKOFF = 5.0
        
        for attempt in range(MAX_RETRIES):
            # (Bonus-2) Randomized delay before each attempt
            if attempt == 0:
                # Initial jitter to break symmetry
                jitter = random.uniform(0, 0.1)
                time.sleep(jitter)
                print(f"[PROPOSER {self.node_id}] Attempt {attempt + 1}/{MAX_RETRIES} (initial jitter: {jitter:.3f}s)")
            else:
                # Exponential backoff with randomization on retry
                backoff = min(MIN_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
                backoff_with_jitter = backoff * random.uniform(0.5, 1.5)
                print(f"[PROPOSER {self.node_id}] Attempt {attempt + 1}/{MAX_RETRIES} after {backoff_with_jitter:.3f}s backoff")
                time.sleep(backoff_with_jitter)

            # --- Step 1: Choose new proposal number n ---
            with self.lock:
                self.proposal_counter += 1
                proposal_id = (self.proposal_counter, self.node_id)

            print(f"[PROPOSER {self.node_id}] Starting Phase 1 (PREPARE) with ID {proposal_id}")

            # --- Step 2: Broadcast Prepare(n) to all servers ---
            responses = self._broadcast_rpc('rpc_prepare', proposal_id)

            # --- Step 4: When responses received from majority ---
            promises = [r for r in responses.values() if r and r.get('status') == 'promise']

            if len(promises) < MAJORITY:
                print(f"[PROPOSER {self.node_id}] Phase 1 FAILED: Not enough promises ({len(promises)}/{MAJORITY}).")
                if attempt < MAX_RETRIES - 1:
                    print(f"[PROPOSER {self.node_id}] Will retry with new proposal ID...")
                    continue  # Retry with new proposal ID
                else:
                    print(f"[PROPOSER {self.node_id}] Max retries reached. Giving up.")
                    return {"status": "fail", "reason": "prepare_rejected", "proposal_id": proposal_id, "attempts": attempt + 1}

            print(f"[PROPOSER {self.node_id}] Phase 1 SUCCESS: Got {len(promises)} promises.")

            # --- Step 4 logic: Check for accepted values ---
            highest_accepted_id = (-1, -1)
            value_to_propose = value

            for res in promises:
                if res['accepted_id'] > highest_accepted_id:
                    highest_accepted_id = res['accepted_id']
                    value_to_propose = res['accepted_value']

            if value_to_propose != value:
                print(f"[PROPOSER {self.node_id}] Previous value '{value_to_propose}' (ID {highest_accepted_id}) found. Must propose it.")

            # --- Step 5: Broadcast Accept(n, value) to all servers ---
            print(f"[PROPOSER {self.node_id}] Starting Phase 2 (ACCEPT) with ID {proposal_id} and value '{value_to_propose}'")
            responses = self._broadcast_rpc('rpc_accept', proposal_id, value_to_propose)

            # --- Step 7: When responses received from majority ---
            accepts = [r for r in responses.values() if r and r.get('status') == 'accepted']

            if len(accepts) < MAJORITY:
                print(f"[PROPOSER {self.node_id}] Phase 2 FAILED: Not enough accepts ({len(accepts)}/{MAJORITY}).")
                if attempt < MAX_RETRIES - 1:
                    print(f"[PROPOSER {self.node_id}] Will retry with new proposal ID...")
                    continue  # Retry with new proposal ID
                else:
                    print(f"[PROPOSER {self.node_id}] Max retries reached. Giving up.")
                    return {"status": "fail", "reason": "accept_rejected", "proposal_id": proposal_id, "attempts": attempt + 1}

            # --- SUCCESS! Value is chosen ---
            print(f"[PROPOSER {self.node_id}] Phase 2 SUCCESS: Value '{value_to_propose}' is CHOSEN after {attempt + 1} attempt(s).")

            # Inform all nodes (acting as Learners) of the final value
            self._broadcast_rpc('rpc_learn_chosen_value', value_to_propose)

            return {"status": "success", "value": value_to_propose, "attempts": attempt + 1}

        # Should never reach here, but just in case
        return {"status": "fail", "reason": "max_retries_exceeded", "attempts": MAX_RETRIES}

    def rpc_learn_chosen_value(self, value):  # Learner hook invoked once consensus succeeds
        """
        This is our simple "Learner" logic. When a Proposer succeeds,
        it tells everyone what the value is.
        """
        with self.lock:  # Synchronize learner updates
            if self.chosen_value is None:  # Only learn once
                self.chosen_value = value  # Record chosen value
                self._write_to_file(value)  # Persist to disk
                self._persist_acceptor_state()  # Ensure persisted metadata reflects final value
                print(f"  [LEARNER {self.node_id}] Learned CHOSEN value: '{value}'. Wrote to file.")  # Log learn event
        return True  # Return success to caller

    def _broadcast_rpc(self, func_name, *args):  # Fan out RPC call to peers and local instance
        """Helper to send an RPC to all peers and self."""
        responses = {}  # Collect response payloads keyed by node ID

        # Call peers
        for peer_id, proxy in self.peers.items():  # Iterate through connected proxies
            try:
                responses[peer_id] = getattr(proxy, func_name)(*args)  # Dispatch remote RPC
            except Exception as e:
                print(f"[ERROR] RPC call {func_name} to {peer_id} failed: {e}")  # Record failure
                responses[peer_id] = None  # Placeholder for failed call

        # Call self (no network needed)
        try:
            local_func = getattr(self, func_name)  # Resolve local handler
            responses[self.node_id] = local_func(*args)  # Execute locally
        except Exception as e:
            print(f"[ERROR] Local call {func_name} failed: {e}")  # Report local failure
            responses[self.node_id] = None  # Preserve map shape

        return responses  # Return aggregated responses


def main():
    if len(sys.argv) < 2:  # Require a node identifier argument
        print("Usage: python node.py <node_id>")  # Provide usage instructions
        print("Example: python node.py 1")  # Offer concrete example
        sys.exit(1)  # Exit because arguments are missing

    try:
        node_id = int(sys.argv[1])  # Parse node identifier
        if node_id not in PEERS_CONFIG:  # Validate ID exists in configuration
            raise ValueError  # Trigger error for unsupported ID
    except ValueError:
        print(f"Invalid node_id. Must be one of {list(PEERS_CONFIG.keys())}")  # Report invalid ID
        sys.exit(1)  # Abort startup

    node = PaxosNode(node_id)  # Instantiate Paxos node with validated ID

    # Start a thread to connect to peers
    peer_thread = threading.Thread(target=node.connect_to_peers)  # Prepare background connector thread
    peer_thread.daemon = True  # Allow program to exit even if thread is running
    peer_thread.start()  # Begin asynchronous peer connections

    # Register all RPC functions
    handler = rpc_tools.RPCHandler()  # Create server-side dispatcher
    handler.register_function(node.rpc_prepare)  # Expose prepare RPC endpoint
    handler.register_function(node.rpc_accept)  # Expose accept RPC endpoint
    handler.register_function(node.rpc_submit_value)  # Expose proposer entry point
    handler.register_function(node.rpc_get_file_content)  # Expose file read helper
    handler.register_function(node.rpc_learn_chosen_value)  # Expose learner notification
    handler.register_function(node.rpc_reset_state)  # Expose reset for testing

    # Start the RPC server
    try:
        rpc_tools.rpc_server(handler, node.address, authkey=AUTHKEY)  # Launch blocking RPC listener
    except KeyboardInterrupt:
        print("\nShutting down server...")  # Gracefully exit on Ctrl+C
    except Exception as e:
        print(f"Server crashed: {e}")  # Report unexpected fatal error


if __name__ == "__main__":  # Only execute when run as script
    main()  # Delegate to CLI entry point