"""Client for running Paxos test scenarios."""

import sys
import threading
import time
from multiprocessing.connection import Client
import rpc_tools

# Configuration - must match node.py
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),
    2: ('10.128.0.4', 17002),
    3: ('10.128.0.6', 17003),
}
AUTHKEY = b'paxos_lab_secret'


# ============================================================================
# Helper Functions
# ============================================================================

def _rpc_call(node_id, method_name, *args):
    """Generic RPC call helper."""
    addr = PEERS_CONFIG[node_id]
    conn = Client(addr, authkey=AUTHKEY)
    try:
        proxy = rpc_tools.RPCProxy(conn)
        return getattr(proxy, method_name)(*args)
    finally:
        conn.close()


def _submit(node_id, value, delay, enable_retry=False):
    """Submit a value proposal to a node."""
    print(f"Submitting '{value}' to Node {node_id} (delay {delay}s)")
    result = _rpc_call(node_id, 'rpc_submit_value', value, delay, enable_retry)
    print(f" -> {result}")
    return result


def _show_cluster_state():
    """Display the chosen value on all nodes."""
    print("\n=== Cluster state ===")
    for node_id in sorted(PEERS_CONFIG):
        state = _rpc_call(node_id, 'rpc_get_file_content')
        print(f"Node {node_id} value: {state}")
    print("=====================\n")


def _reset_cluster():
    """Reset all nodes to initial state."""
    print("\n=== Resetting cluster state ===")
    for node_id in sorted(PEERS_CONFIG):
        try:
            result = _rpc_call(node_id, 'rpc_reset_state')
            print(f"Node {node_id} reset: {result.get('status', 'unknown')}")
        except Exception as e:
            print(f"Error resetting Node {node_id}: {e}")
    print("=== Reset complete ===\n")
    time.sleep(0.5)


def _run_concurrent_proposals(proposals):
    """Run multiple proposals concurrently using threads."""
    threads = [threading.Thread(target=_submit, args=p) for p in proposals]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ============================================================================
# Scenario Implementations
# ============================================================================

def scenario_1_single():
    """Single proposer with no contention."""
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 1: Single Proposer")
    print("Expected: One proposal succeeds with ValueA")
    print("="*70 + "\n")
    
    _submit(1, "ValueA", 0.0)
    time.sleep(1)
    _show_cluster_state()


def scenario_2_a_wins():
    """Sequential proposals where first proposer completes before second starts."""
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 2: A Wins (Page 23 - Previous value already chosen)")
    print("Expected: A completes first, B sees A's value and adopts it")
    print("="*70 + "\n")
    
    _submit(1, "ValueA", 0.0)
    time.sleep(0.5)
    _submit(2, "ValueB", 0.0)
    time.sleep(1)
    _show_cluster_state()


def scenario_3_b_sees_a():
    """Overlapping proposals where second proposer sees first's value."""
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 3: B Wins (Page 24 - Previous value not chosen, but B sees it)")
    print("Expected: A starts first, B sees A's value and adopts it")
    print("="*70 + "\n")
    
    _run_concurrent_proposals([
        (1, "ValueA", 0.05, False),  # A starts first (50ms delay)
        (2, "ValueB", 0.1, False),   # B starts second (100ms delay)
    ])
    time.sleep(1)
    _show_cluster_state()


def scenario_4_b_doesnt_see_a():
    """True interleaving where second proposer doesn't see first's value."""
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 4: B Wins (Page 25 - Previous value not chosen, B doesn't see it)")
    print("Expected: True interleaving, B proposes its own value")
    print("="*70 + "\n")
    
    _run_concurrent_proposals([
        (1, "ValueA", 0.01, False),  # Very close timing
        (2, "ValueB", 0.0, False),   # B starts slightly first
    ])
    time.sleep(1)
    _show_cluster_state()


def scenario_5_livelock():
    """Multiple competing proposers with retry to prevent livelock."""
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 5: Livelock Prevention (Page 26)")
    print("Expected: Three proposers compete, retries with exponential backoff")
    print("="*70 + "\n")
    
    _run_concurrent_proposals([
        (1, "ValueA", 0.1, True),  # enable_retry=True
        (2, "ValueB", 0.1, True),
        (3, "ValueC", 0.1, True),
    ])
    time.sleep(1)
    _show_cluster_state()


# ============================================================================
# Command-Line Interface
# ============================================================================

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
    if len(sys.argv) < 2:
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
    
    scenario_name = sys.argv[1].lower()
    scenario_func = SCENARIOS.get(scenario_name)
    
    if scenario_func is None:
        print(f"Unknown scenario: {scenario_name}")
        print("\nAvailable scenarios:")
        for key in sorted(set(SCENARIOS.keys())):
            if not key.isdigit():
                print(f"  {key}")
        sys.exit(1)
    
    scenario_func()


if __name__ == "__main__":
    main()