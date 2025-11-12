"""Client helper for issuing Paxos proposals and running canned scenarios."""

import argparse  # Provides CLI parsing for scenario support
import sys  # Provides access to command-line arguments (required by argparse)
import threading  # Used to run competing proposers concurrently
import time  # Enables inter-scenario sleeps
from multiprocessing.connection import Client  # Enables authenticated IPC connections
import rpc_tools  # Imports shared RPC helper utilities

# !! IMPORTANT !!
# This config must match the one in node.py
PEERS_CONFIG = {  # Maps node IDs to (host, port) tuples for each Paxos node
    1: ('10.128.0.3', 17001),  # Node 1 address
    2: ('10.128.0.4', 17002),  # Node 2 address
    3: ('10.128.0.6', 17003),  # Node 3 address
}
AUTHKEY = b'paxos_lab_secret'  # Shared authentication token for all RPC connections


def _connect(node_id):  # Helper to open RPC connection to single node
    addr = PEERS_CONFIG[node_id]
    conn = Client(addr, authkey=AUTHKEY)
    return rpc_tools.RPCProxy(conn), conn


def _submit(node_id, value, delay):  # Issue submit RPC and print result
    proxy, conn = _connect(node_id)
    try:
        print(f"Submitting '{value}' to Node {node_id} (delay {delay}s)")
        result = proxy.rpc_submit_value(value, delay)
        print(f" -> {result}")
        return result
    finally:
        conn.close()


def _get_state(node_id):  # Query node state for inspection
    proxy, conn = _connect(node_id)
    try:
        state = proxy.rpc_get_file_content()
        print(f"Node {node_id} value: {state}")
        return state
    finally:
        conn.close()


def _show_cluster_state():  # Print current learner values for all nodes
    print("\n=== Cluster state ===")
    for node_id in sorted(PEERS_CONFIG):
        _get_state(node_id)
    print("=====================\n")


def run_scenario_single():  # Scenario 1 from reference client
    print("\nScenario 1: Single proposer\n")
    _submit(1, "ValueA", 0.0)
    time.sleep(1)
    _show_cluster_state()


def run_scenario_a_wins():  # Scenario 2
    print("\nScenario 2: Two proposers, A wins\n")
    _submit(1, "ValueA", 0.0)
    time.sleep(0.5)
    _submit(2, "ValueB", 0.0)
    time.sleep(1)
    _show_cluster_state()


def run_scenario_b_wins_seen():  # Scenario 3
    print("\nScenario 3: Two proposers, B wins (A delayed)\n")

    threads = [
        threading.Thread(target=_submit, args=(1, "ValueA", 0.3)),
        threading.Thread(target=_submit, args=(2, "ValueB", 0.0)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(1)
    _show_cluster_state()


def run_scenario_b_wins_interleaved():  # Scenario 4
    print("\nScenario 4: Two proposers, interleaved\n")
    threads = [
        threading.Thread(target=_submit, args=(1, "ValueA", 0.15)),
        threading.Thread(target=_submit, args=(2, "ValueB", 0.05)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(1)
    _show_cluster_state()


def run_scenario_livelock():  # Scenario 5
    print("\nScenario 5: Three proposers (livelock stress)\n")
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


SCENARIOS = {  # Mapping of scenario identifiers to runner functions
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
}


def run_direct_submit(args):  # Preserve original direct-submit behavior
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


def build_parser():  # Configure argparse CLI
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

    parser.add_argument(
        "fallback_args",
        nargs="*",
        help=argparse.SUPPRESS,
    )
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
            command="submit", node_id=legacy_args[0], value=legacy_args[1],
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


if __name__ == "__main__":  # Execute only when run as script
    main()  # Kick off CLI workflow