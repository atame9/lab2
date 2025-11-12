"""Client helper for issuing Paxos proposals and running canned scenarios."""

import argparse
import sys
import threading
import time
from multiprocessing.connection import Client
import rpc_tools

# !! IMPORTANT !!
# This config must match the one in node.py
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),
    2: ('10.128.0.4', 17002),
    3: ('10.128.0.6', 17003),
}
AUTHKEY = b'paxos_lab_secret'


def _connect(node_id):
    """Helper to open RPC connection to single node."""
    addr = PEERS_CONFIG[node_id]
    conn = Client(addr, authkey=AUTHKEY)
    return rpc_tools.RPCProxy(conn), conn


def _submit(node_id, value, delay):
    """Issue submit RPC and print result."""
    proxy, conn = _connect(node_id)
    try:
        print(f"Submitting '{value}' to Node {node_id} (delay {delay}s)")
        result = proxy.rpc_submit_value(value, delay)
        print(f" -> {result}")
        return result
    finally:
        conn.close()


def _get_state(node_id):
    """Query node state for inspection."""
    proxy, conn = _connect(node_id)
    try:
        state = proxy.rpc_get_file_content()
        print(f"Node {node_id} value: {state}")
        return state
    finally:
        conn.close()


def _show_cluster_state():
    """Print current learner values for all nodes."""
    print("\n=== Cluster state ===")
    for node_id in sorted(PEERS_CONFIG):
        _get_state(node_id)
    print("=====================\n")


def _reset_node(node_id):
    """Reset a single node to initial state."""
    proxy, conn = _connect(node_id)
    try:
        result = proxy.rpc_reset_state()
        print(f"Node {node_id} reset: {result.get('status', 'unknown')}")
        return result
    finally:
        conn.close()


def _reset_cluster():
    """Reset all nodes to fresh state for testing."""
    print("\n=== Resetting cluster state ===")
    for node_id in sorted(PEERS_CONFIG):
        try:
            _reset_node(node_id)
        except Exception as e:
            print(f"Error resetting Node {node_id}: {e}")
    print("=== Reset complete ===\n")
    time.sleep(0.5)


# ============================================================================
# SCENARIO IMPLEMENTATIONS (Based on Lab PDF and Slides)
# ============================================================================

def run_scenario_single():
    """
    Requirement #4 (50%): Single proposer and success message to client
    
    Slide Reference: Basic Paxos with no contention
    Expected Outcome: One proposal succeeds cleanly
    """
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 1: Single Proposer (Requirement #4)")
    print("Expected: One proposal succeeds with ValueA")
    print("="*70 + "\n")
    
    _submit(1, "ValueA", 0.0)
    time.sleep(1)
    _show_cluster_state()


def run_scenario_a_wins():
    """
    Requirement #5 (10%): A wins
    
    Slide Reference: Page 23 - "Previous value already chosen"
    Timeline:
      - A proposes ValueA, completes both phases
      - B proposes ValueB later
      - B sees ValueA was already chosen
      - B adopts ValueA (not ValueB)
    
    Expected Outcome: Both succeed with ValueA
    """
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 2: A Wins (Requirement #5 - Slide Page 23)")
    print("Concept: Previous value already chosen")
    print("Expected: A completes first, B sees A's value and adopts it")
    print("Result: Both succeed with ValueA (not ValueB)")
    print("="*70 + "\n")
    
    _submit(1, "ValueA", 0.0)
    time.sleep(0.5)  # Ensure A finishes completely before B starts
    _submit(2, "ValueB", 0.0)
    time.sleep(1)
    _show_cluster_state()


def run_scenario_b_wins_seen():
    """
    Requirement #6 (10%): B wins in scenario on page 24
    
    Slide Reference: Page 24 - "Previous value not chosen, but new proposer sees it"
    Timeline:
      - A proposes ValueA, gets some accepts (not yet majority/chosen)
      - B proposes ValueB with slightly earlier timing
      - B does Phase 1, sees that A's value was accepted on some nodes
      - B adopts ValueA (even though B wanted ValueB)
      - B succeeds with ValueA, A may fail due to B's higher proposal ID
    
    Expected Outcome: B succeeds with ValueA (adopted from A)
    """
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 3: B Wins - Sees Previous Value (Requirement #6 - Slide Page 24)")
    print("Concept: Previous value not chosen, but new proposer sees it")
    print("Expected: A gets partial accepts, B sees A's value and adopts it")
    print("Result: B succeeds with ValueA (not ValueB), A may fail")
    print("="*70 + "\n")

    threads = [
        threading.Thread(target=_submit, args=(1, "ValueA", 0.1)),   # A: 100ms delay
        threading.Thread(target=_submit, args=(2, "ValueB", 0.05)),  # B: 50ms delay (starts slightly first)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(1)
    _show_cluster_state()


def run_scenario_b_wins_interleaved():
    """
    Bonus-1 / Requirement #7 (5%): B wins in scenario on page 25
    
    Slide Reference: Page 25 - "Previous value not chosen, new proposer doesn't see it"
    Timeline:
      - A proposes ValueA, starts getting accepts
      - B proposes ValueB almost simultaneously (true interleaving)
      - B does Phase 1 on servers that HAVEN'T accepted A yet
      - B doesn't see A's value (those servers have no accepted value)
      - B proposes its own ValueB
      - B's higher proposal ID blocks A
      - B succeeds with ValueB
    
    Expected Outcome: B succeeds with ValueB (its own value), A fails
    """
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 4: B Wins - Doesn't See Previous (Bonus-1 - Slide Page 25)")
    print("Concept: Previous value not chosen, new proposer doesn't see it")
    print("Expected: True interleaving, B queries servers that never saw A")
    print("Result: B proposes ValueB (its own value), A gets blocked")
    print("="*70 + "\n")
    
    threads = [
        threading.Thread(target=_submit, args=(1, "ValueA", 0.01)),  # Very close timing
        threading.Thread(target=_submit, args=(2, "ValueB", 0.0)),   # B starts just slightly first
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(1)
    _show_cluster_state()


def run_scenario_livelock():
    """
    Bonus-2 / Requirement #8 (5%): Simulate livelock and solve with randomized restart
    
    Slide Reference: Page 26 - "Liveness" / Livelock
    Concept: Multiple competing proposers can livelock
    Solution: "randomized delay before restarting"
    
    Timeline:
      - Three proposers start nearly simultaneously
      - They interfere with each other's proposals
      - Random jitter (0-100ms) in node.py breaks the symmetry
      - Eventually one proposal succeeds
    
    Note: Your code has random jitter (line 203 in node.py) but NOT retry logic.
          True "randomized restart" would require proposals to retry after failure
          with exponential backoff. Current implementation uses jitter to prevent
          livelock initially, but doesn't retry on failure.
    
    Expected Outcome: One value chosen despite three competing proposers
    """
    _reset_cluster()
    print("\n" + "="*70)
    print("SCENARIO 5: Livelock Prevention (Bonus-2 - Slide Page 26)")
    print("Concept: Competing proposers can livelock")
    print("Solution: Random jitter prevents livelock")
    print("Expected: Three proposers compete, one value eventually chosen")
    print()
    print("NOTE: Current implementation uses random jitter (0-100ms) to break")
    print("      symmetry, but does NOT have retry logic. True 'randomized restart'")
    print("      would require failed proposals to retry with random backoff.")
    print("="*70 + "\n")
    
    threads = [
        threading.Thread(target=_submit, args=(1, "ValueA", 0.1)),
        threading.Thread(target=_submit, args=(2, "ValueB", 0.1)),
        threading.Thread(target=_submit, args=(3, "ValueC", 0.1)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(1)
    _show_cluster_state()


# ============================================================================
# COMMAND-LINE INTERFACE
# ============================================================================

SCENARIOS = {
    "1": run_scenario_single,
    "single": run_scenario_single,
    "2": run_scenario_a_wins,
    "a_wins": run_scenario_a_wins,
    "3": run_scenario_b_wins_seen,
    "b_wins_seen": run_scenario_b_wins_seen,
    "4": run_scenario_b_wins_interleaved,
    "b_wins_interleaved": run_scenario_b_wins_interleaved,
    "5": run_scenario_livelock,
    "livelock": run_scenario_livelock,
    "state": _show_cluster_state,
    "reset": _reset_cluster,
}


def run_direct_submit(args):
    """Preserve original direct-submit behavior."""
    try:
        node_id = int(args.node_id)
        if node_id not in PEERS_CONFIG:
            raise ValueError(f"Node ID not in config: {node_id}")
    except ValueError as exc:
        print(f"Invalid node_id: {exc}")
        sys.exit(1)

    try:
        delay = float(args.delay) if args.delay is not None else 0.0
    except ValueError:
        print("Delay must be numeric.")
        sys.exit(1)

    _submit(node_id, args.value, delay)


def build_parser():
    """Configure argparse CLI."""
    parser = argparse.ArgumentParser(description="Paxos test client with scenarios")
    sub = parser.add_subparsers(dest="command")

    scenario = sub.add_parser("scenario", help="Run a predefined scenario")
    scenario.add_argument("name", help="Scenario name or number")

    state = sub.add_parser("state", help="Show cluster state")
    state.set_defaults(command="scenario", name="state")

    submit = sub.add_parser("submit", help="Submit a value directly")
    submit.add_argument("node_id", help="Target node id (1..N)")
    submit.add_argument("value", help="Value to propose")
    submit.add_argument("--delay", help="Optional proposer delay in seconds")

    parser.add_argument("fallback_args", nargs="*", help=argparse.SUPPRESS)
    return parser


def main():
    parser = build_parser()
    if len(sys.argv) > 1 and sys.argv[1] not in {"scenario", "state", "submit"}:
        # Support legacy positional usage: node_id value [delay]
        legacy_args = sys.argv[1:]
        if len(legacy_args) < 2:
            parser.print_help()
            sys.exit(1)
        namespace = argparse.Namespace(
            command="submit",
            node_id=legacy_args[0],
            value=legacy_args[1],
            delay=legacy_args[2] if len(legacy_args) > 2 else None
        )
    else:
        namespace = parser.parse_args()
        if namespace.command is None:
            parser.print_help()
            sys.exit(1)

    if namespace.command == "submit":
        run_direct_submit(namespace)
    else:
        scenario_name = namespace.name.lower()
        runner = SCENARIOS.get(scenario_name)
        if runner is None:
            print("Unknown scenario. Available options:")
            for key in sorted({k for k in SCENARIOS if not k.isdigit()}):
                print(f"  - {key}")
            sys.exit(1)
        runner()


if __name__ == "__main__":
    main()