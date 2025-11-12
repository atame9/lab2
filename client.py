"""Client for running Paxos test scenarios."""

import sys  # For command-line argument parsing
import threading  # For concurrent proposal submissions
import time  # For delays between operations
from multiprocessing.connection import Client  # For establishing RPC connections to nodes
import rpc_tools  # Custom RPC utilities for remote procedure calls

# Configuration - must match node.py exactly for proper communication
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),  # Node 1: IP address and port
    2: ('10.128.0.4', 17002),  # Node 2: IP address and port
    3: ('10.128.0.6', 17003),  # Node 3: IP address and port
}
AUTHKEY = b'paxos_lab_secret'  # Shared secret for authenticated connections


# ============================================================================
# Helper Functions
# ============================================================================

def _rpc_call(node_id, method_name, *args):
    """Generic RPC call helper."""
    # Get the network address (IP, port) for the target node
    addr = PEERS_CONFIG[node_id]
    
    # Establish connection to the node using authentication key
    conn = Client(addr, authkey=AUTHKEY)
    try:
        # Create RPC proxy for remote method invocation
        proxy = rpc_tools.RPCProxy(conn)
        
        # Call the remote method with provided arguments
        return getattr(proxy, method_name)(*args)
    finally:
        # Always close connection to free resources
        conn.close()


def _submit(node_id, value, delay, enable_retry=False, inter_phase_delay=0.0, phase2_accept_delays=None):
    """Submit a value proposal to a node."""
    # Print submission details for user visibility
    print(f"Submitting '{value}' to Node {node_id} (delay {delay}s)")
    
    # Make RPC call to node's submit_value method with all parameters
    result = _rpc_call(node_id, 'rpc_submit_value', value, delay, enable_retry, inter_phase_delay, phase2_accept_delays)
    
    # Display the result (success/failure) from the proposer
    print(f" -> {result}")
    return result


def _show_cluster_state():
    """Display the chosen value on all nodes."""
    print("\n=== Cluster state ===")
    
    # Query each node for its current file content
    for node_id in sorted(PEERS_CONFIG):
        # Get the value that this node has written to its file
        state = _rpc_call(node_id, 'rpc_get_file_content')
        print(f"Node {node_id} value: {state}")
    
    print("=====================\n")


def _reset_cluster():
    """Reset all nodes to initial state."""
    print("\n=== Resetting cluster state ===")
    
    # Send reset command to each node in the cluster
    for node_id in sorted(PEERS_CONFIG):
        try:
            # Call the reset RPC method on this node
            result = _rpc_call(node_id, 'rpc_reset_state')
            
            # Display success/failure status for this node
            print(f"Node {node_id} reset: {result.get('status', 'unknown')}")
        except Exception as e:
            # Handle connection or RPC errors gracefully
            print(f"Error resetting Node {node_id}: {e}")
    
    print("=== Reset complete ===\n")
    
    # Brief pause to let reset operations complete
    time.sleep(0.5)


def _run_concurrent_proposals(proposals):
    """Run multiple proposals concurrently using threads."""
    # Create a thread for each proposal to simulate concurrent proposers
    threads = [threading.Thread(target=_submit, args=p) for p in proposals]
    
    # Start all threads simultaneously
    for t in threads:
        t.start()
    
    # Wait for all threads to complete before returning
    for t in threads:
        t.join()


# ============================================================================
# Scenario Implementations
# ============================================================================

def scenario_1_single():
    """Single proposer with no contention."""
    # Start fresh by resetting all nodes
    _reset_cluster()
    
    # Display scenario description
    print("\n" + "="*70)
    print("SCENARIO 1: Single Proposer")
    print("Expected: One proposal succeeds with ValueA")
    print("="*70 + "\n")
    
    # Submit single proposal to Node 1 with no delay
    _submit(1, "ValueA", 0.0)
    
    # Wait for consensus to complete
    time.sleep(1)
    
    # Show final state across all nodes (should all have ValueA)
    _show_cluster_state()


def scenario_2_a_wins():
    """Sequential proposals where first proposer completes before second starts."""
    # Start fresh by resetting all nodes
    _reset_cluster()
    
    # Display scenario description (references Paxos paper page 23)
    print("\n" + "="*70)
    print("SCENARIO 2: A Wins (Page 23 - Previous value already chosen)")
    print("Expected: A completes first, B sees A's value and adopts it")
    print("="*70 + "\n")
    
    # First proposal starts and completes
    _submit(1, "ValueA", 0.0)
    
    # Wait for ValueA to be chosen before starting second proposal
    time.sleep(0.5)
    
    # Second proposal should see ValueA was already chosen and adopt it
    _submit(2, "ValueB", 0.0)
    
    # Wait for second proposal to complete
    time.sleep(1)
    
    # Show final state (all nodes should have ValueA, not ValueB)
    _show_cluster_state()


def scenario_3_b_sees_a():
    """Overlapping proposals where second proposer sees first's value."""
    # Start fresh by resetting all nodes
    _reset_cluster()
    
    # Display scenario description (references Paxos paper page 24)
    print("\n" + "="*70)
    print("SCENARIO 3: B Sees A (Page 24 - Previous value not chosen, but B sees it)")
    print("Expected: Node 1's Phase 2 delayed to Nodes 1&2, Node 3 accepts first")
    print("          Node 2 prepares after Node 3 accepts, sees the value")
    print("="*70 + "\n")
    
    # Recreate the screenshot scenario:
    # - Node 1 (acting as S3 in diagram): Proposes ValueA
    #   - Phase 1 completes on all nodes
    #   - Phase 2 ACCEPT is delayed to Nodes 1 & 2 (simulate network delay)
    #   - Phase 2 ACCEPT reaches Node 3 immediately
    # - Node 2 (acting as S5 in diagram): Proposes ValueB after Node 3 accepts
    #   - Sees Node 3's accepted value and adopts it
    
    _run_concurrent_proposals([
        # Node 1: Delay Phase 2 to nodes 1 & 2, so only node 3 accepts immediately
        (1, "ValueA", 0.0, False, 0.0, {1: 0.1, 2: 0.1}),
        
        # Node 2: Starts after node 3 has accepted, will see the accepted value
        (2, "ValueB", 0.03, False, 0.0, None),
    ])
    
    # Wait for both proposals to complete
    time.sleep(1)
    
    # Show final state (all nodes should have ValueA)
    _show_cluster_state()


def scenario_4_b_doesnt_see_a():
    """True interleaving where second proposer doesn't see first's value."""
    # Start fresh by resetting all nodes
    _reset_cluster()
    
    # Display scenario description (references Paxos paper page 25)
    print("\n" + "="*70)
    print("SCENARIO 4: B Wins (Page 25 - Previous value not chosen, B doesn't see it)")
    print("Expected: True interleaving, B proposes its own value")
    print("="*70 + "\n")
    
    # Run proposals concurrently with very close timing
    _run_concurrent_proposals([
        (1, "ValueA", 0.01, False),  # Very close timing (10ms delay)
        (2, "ValueB", 0.0, False),   # B starts slightly first (no delay)
    ])
    
    # Wait for both proposals to complete
    time.sleep(1)
    
    # Show final state (winner depends on exact timing and interleaving)
    _show_cluster_state()


def scenario_5_livelock():
    """Multiple competing proposers with retry to prevent livelock."""
    # Start fresh by resetting all nodes
    _reset_cluster()
    
    # Display scenario description (references Paxos paper page 26)
    print("\n" + "="*70)
    print("SCENARIO 5: Livelock Prevention (Page 26)")
    print("Expected: Three proposers compete, retries with exponential backoff")
    print("="*70 + "\n")
    
    # Run three concurrent proposals, all with retry enabled
    _run_concurrent_proposals([
        (1, "ValueA", 0.1, True),  # enable_retry=True for livelock prevention
        (2, "ValueB", 0.1, True),  # enable_retry=True for livelock prevention
        (3, "ValueC", 0.1, True),  # enable_retry=True for livelock prevention
    ])
    
    # Wait for proposals to complete (may take longer due to retries)
    time.sleep(1)
    
    # Show final state (one value should win after retries)
    _show_cluster_state()


# ============================================================================
# Command-Line Interface
# ============================================================================

# Map user-friendly scenario names to their implementation functions
SCENARIOS = {
    '1': scenario_1_single,
    'single': scenario_1_single,
    '2': scenario_2_a_wins,
    'a_wins': scenario_2_a_wins,
    '3': scenario_3_b_sees_a,
    'b_sees_a': scenario_3_b_sees_a,
    '4': scenario_4_b_doesnt_see_a,
    'b_doesnt_see_a': scenario_4_b_doesnt_see_a,
    '5': scenario_5_livelock,
    'livelock': scenario_5_livelock,
    'state': _show_cluster_state,
    'reset': _reset_cluster,
}


def main():
    # Check if user provided a scenario argument
    if len(sys.argv) < 2:
        # Display usage instructions if no argument provided
        print("Usage: python3 client.py <scenario>")
        print("\nAvailable scenarios:")
        print("  1, single              - Single proposer")
        print("  2, a_wins              - A wins (Page 23)")
        print("  3, b_sees_a            - B sees A's value (Page 24)")
        print("  4, b_doesnt_see_a      - B doesn't see A (Page 25)")
        print("  5, livelock            - Livelock prevention (Page 26)")
        print("  state                  - Show cluster state")
        print("  reset                  - Reset cluster")
        sys.exit(1)
    
    # Get the scenario name from command line (case-insensitive)
    scenario_name = sys.argv[1].lower()
    
    # Look up the corresponding scenario function
    scenario_func = SCENARIOS.get(scenario_name)
    
    # Handle invalid scenario names
    if scenario_func is None:
        print(f"Unknown scenario: {scenario_name}")
        print("\nAvailable scenarios:")
        
        # Display all unique scenario names
        for key in sorted(set(SCENARIOS.keys())):
            if not key.isdigit():  # Skip numeric aliases
                print(f"  {key}")
        sys.exit(1)
    
    # Execute the requested scenario
    scenario_func()


if __name__ == "__main__":
    main()