#!/usr/bin/env python3
"""Shared host-side lifecycle for the two k3d CPU Manager shell helpers.

Kubelet configuration belongs to nodes, outside Dimster's manifest deployment.
Keep that lifecycle here: preflight, drain, stop, reset checkpoint, start, verify.
Only Python's standard library, Docker, and kubectl are required.
"""

import argparse
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time


CONFIG_DIR = "/etc/rancher/k3s/config.yaml.d"
CONFIG_NAME = "zz-dimster-cpu-manager.yaml"
CONFIG_PATH = f"{CONFIG_DIR}/{CONFIG_NAME}"
OWNER = "# Managed by Dimster k3d CPU Manager helpers.\n"


def run(*args, timeout=60):
    """Capture command output so node configuration and credentials stay private."""
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} {args[1]} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def kubectl(context, *args, timeout=60):
    return run("kubectl", "--context", context, "--request-timeout=20s", *args, timeout=timeout)


def cpus(value):
    """Expand Linux cpulists without assuming SMT siblings have adjacent IDs."""
    result = set()
    if not re.fullmatch(r"\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*", value):
        raise ValueError(f"Invalid CPU list: {value!r}")
    for part in value.split(","):
        bounds = [int(x) for x in part.split("-")]
        first, last = bounds[0], bounds[-1]
        if first > last or last > 1048576:
            raise ValueError(f"Invalid CPU range: {part}")
        result.update(range(first, last + 1))
    return result


def select_reserved(topology, requested, full_cores):
    lines = topology.splitlines()
    allowed = cpus(lines[0])
    cores = {frozenset(cpus(line)) for line in lines[1:] if line}
    complete = sorted((core for core in cores if core <= allowed), key=lambda c: min(c))
    if not complete:
        raise ValueError("The node has no complete physical core available for reservation")
    selected = set(complete[0]) if requested == "auto" else cpus(requested)
    if not selected < allowed:
        raise ValueError("Reserved CPUs must be available to the node and leave CPUs for workloads")
    if full_cores and any(core & selected and not core <= selected for core in cores):
        raise ValueError("With full cores enabled, --reserved-cpus must include every SMT sibling")
    widths = {len(core) for core in cores if core & allowed}
    if full_cores and len(widths) != 1:
        raise ValueError("Mixed SMT widths are unsupported; use --allow-smt-sharing")
    return ",".join(map(str, sorted(selected))), max(widths)


def render_config(policy, reserved=None, full_cores=True):
    # K3s accepts JSON as YAML. '+' preserves unrelated kubelet arguments.
    args = [f"cpu-manager-policy={policy}"]
    if policy == "static":
        args += [f"cpu-manager-policy-options=full-pcpus-only={str(full_cores).lower()}",
                 f"reserved-cpus={reserved}"]
    return OWNER + json.dumps({"kubelet-arg+": args}, indent=2) + "\n"


def read_configs(node):
    # Control-character separators cannot occur in valid YAML text.
    data = run("docker", "exec", node, "sh", "-c", r'''
for file in /etc/rancher/k3s/config.yaml /etc/rancher/k3s/config.yaml.d/*.yaml; do
    [ -f "$file" ] || continue
    printf '\036%s\037' "$file"
    cat "$file" || exit 1
done
''')
    return dict(part.split("\x1f", 1) for part in data.split("\x1e") if part)


def validate_configuration(container, configs):
    command = container["Config"].get("Cmd") or []
    env = container["Config"].get("Env") or []
    # CLI list flags replace config-file lists, so appending a drop-in is unsafe.
    if any(arg.split("=", 1)[0] in ("--kubelet-arg", "--config", "-c") for arg in command):
        raise ValueError("Node startup uses --kubelet-arg/--config; move those settings to the default K3s config first")
    if any(item.split("=", 1)[0] in ("K3S_CONFIG_FILE", "K3S_KUBELET_ARG") for item in env):
        raise ValueError("Node startup overrides K3s config/kubelet arguments through the environment")
    managed = configs.get(CONFIG_PATH)
    if managed is not None and not managed.startswith(OWNER):
        raise ValueError(f"Refusing to overwrite an unowned file: {CONFIG_PATH}")
    for path, content in configs.items():
        if path == CONFIG_PATH:
            continue
        active = "\n".join(line.split("#", 1)[0] for line in content.splitlines())
        if re.search(r"cpu-manager-policy|reserved-cpus|root-dir\s*=|config(?:-dir)?\s*=", active):
            raise ValueError(f"Conflicting kubelet configuration in {path}; manage CPU settings with one mechanism")
        if path > CONFIG_PATH and "kubelet-arg" in active:
            raise ValueError(f"A later drop-in could override these settings: {path}")


def incompatible_pods(pods, width):
    failures = []
    for pod in pods:
        if pod.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        if pod.get("status", {}).get("qosClass") != "Guaranteed":
            continue
        spec = pod["spec"]
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            value = str(container.get("resources", {}).get("requests", {}).get("cpu", "0"))
            try:
                scale = {"m": 1000, "u": 1000000, "n": 1000000000}.get(value[-1:])
                count = Decimal(value[:-1]) / scale if scale else Decimal(value)
            except InvalidOperation as error:
                raise ValueError(f"Cannot parse CPU request {value!r}") from error
            if count > 0 and count == int(count) and int(count) % width:
                metadata = pod["metadata"]
                failures.append(f"{metadata['namespace']}/{metadata['name']}:{container['name']} requests {value} CPUs")
    return failures


def cpu_config(context, node):
    data = json.loads(kubectl(context, "get", "--raw", f"/api/v1/nodes/{node}/proxy/configz"))
    return data.get("kubeletconfig", data.get("kubeletConfig", data))


def signature(config):
    # Include reservations in rollback verification, not just the policy name.
    return (config.get("cpuManagerPolicy", "none"), config.get("cpuManagerPolicyOptions", {}),
            config.get("reservedSystemCPUs", ""), config.get("kubeReserved", {}),
            config.get("systemReserved", {}))


def matches(config, policy, reserved, full_cores):
    if config.get("cpuManagerPolicy") != policy:
        return False
    if policy == "none":
        return not config.get("cpuManagerPolicyOptions")
    full = str(config.get("cpuManagerPolicyOptions", {}).get("full-pcpus-only", "false")).lower() == "true"
    actual_reserved = config.get("reservedSystemCPUs")
    return full == full_cores and bool(actual_reserved) and cpus(actual_reserved) == cpus(reserved)


def preflight(context, node_info, args):
    node = node_info["metadata"]["name"]
    container = json.loads(run("docker", "inspect", node))[0]
    labels = container["Config"].get("Labels") or {}
    if labels.get("k3d.cluster") != args.cluster or labels.get("k3d.role") not in ("server", "agent"):
        raise ValueError(f"{node} is not a server/agent in k3d cluster {args.cluster}")
    if not container["State"]["Running"]:
        raise ValueError(f"{node} must be running before changing its CPU policy")
    volumes = [mount for mount in container["Mounts"]
               if mount["Destination"] == "/var/lib/kubelet" and mount["Type"] == "volume" and mount["RW"]]
    if len(volumes) != 1:
        raise ValueError(f"{node} needs a Docker volume mounted at /var/lib/kubelet")
    configs = read_configs(node)
    validate_configuration(container, configs)
    before = cpu_config(context, node)
    if CONFIG_PATH not in configs and (before.get("cpuManagerPolicy", "none") != "none"
                                      or before.get("cpuManagerPolicyOptions")):
        raise ValueError(f"{node} already has CPU Manager settings outside these helpers")
    reserved, width = None, None
    if args.policy == "static":
        topology = run("docker", "exec", node, "sh", "-c", '''
awk '/^Cpus_allowed_list:/ {print $2}' /proc/self/status
for file in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do cat "$file"; done
''')
        reserved, width = select_reserved(topology, args.reserved_cpus, not args.allow_smt_sharing)
        if not args.allow_smt_sharing:
            pods = json.loads(kubectl(context, "get", "pods", "-A", "--field-selector", f"spec.nodeName={node}", "-o", "json"))["items"]
            failures = incompatible_pods(pods, width)
            if failures:
                raise ValueError("Whole-core allocation would reject these containers:\n  " + "\n  ".join(failures)
                                 + f"\nUse CPU multiples of {width}, fractional shared-pool requests, or --allow-smt-sharing.")
    desired = render_config(args.policy, reserved, not args.allow_smt_sharing)
    previous = configs.get(CONFIG_PATH)
    unchanged = matches(before, args.policy, reserved, not args.allow_smt_sharing) and (
        previous == desired or (args.policy == "none" and previous is None))
    return {"node": node, "image": container["Image"], "volume": volumes[0]["Name"],
            "previous": previous, "before": before, "desired": desired, "reserved": reserved,
            "cordoned": node_info.get("spec", {}).get("unschedulable", False), "unchanged": unchanged}


def install_config(plan, content):
    # docker cp works while stopped and creates/merges this directory only.
    with tempfile.TemporaryDirectory(prefix="dimster-cpu-") as directory:
        folder = Path(directory) / "config.yaml.d"
        folder.mkdir()
        (folder / CONFIG_NAME).write_text(content)
        run("docker", "cp", str(folder), f"{plan['node']}:/etc/rancher/k3s/")


def reset_checkpoint(plan):
    running = json.loads(run("docker", "inspect", "--format", "{{json .State.Running}}", plan["node"]))
    if running:
        raise RuntimeError("Refusing to reset the CPU Manager checkpoint while kubelet is running")
    # Reuse the local node image and mount only the kubelet volume, never host '/'.
    run("docker", "run", "--rm", "--pull=never", "--network=none", "--entrypoint", "/bin/sh",
        "--mount", f"type=volume,src={plan['volume']},dst=/kubelet", plan["image"],
        "-c", "rm -f /kubelet/cpu_manager_state")


def wait_for_config(context, node, predicate, timeout):
    deadline = time.monotonic() + timeout
    last = "API not ready"
    while time.monotonic() < deadline:
        try:
            actual = cpu_config(context, node)
            if predicate(actual):
                remaining = max(1, int(deadline - time.monotonic()))
                kubectl(context, "wait", "--for=condition=Ready", f"node/{node}",
                        f"--timeout={remaining}s", timeout=remaining + 30)
                return
            last = f"effective policy={actual.get('cpuManagerPolicy')}, options={actual.get('cpuManagerPolicyOptions', {})}"
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last = str(error)
        time.sleep(2)
    raise RuntimeError(f"{node} did not become ready with the expected CPU settings: {last}")


def show_logs(node):
    # Docker sends the container's stderr logs to its own stderr.
    subprocess.run(["docker", "logs", "--tail", "30", node],
                   stdout=sys.stderr, stderr=sys.stderr, timeout=30, check=False)


def transition(context, plan, args):
    node = plan["node"]
    if plan["unchanged"]:
        print(f"{node}: already configured; no restart needed", flush=True)
        return
    changed = False
    try:
        print(f"==> Draining {node} (all namespaces)", flush=True)
        drain = ["drain", node, "--ignore-daemonsets", f"--timeout={args.timeout}s"]
        if args.delete_emptydir_data:
            drain.append("--delete-emptydir-data")
        # Do not bypass disruption budgets or force-delete unmanaged pods.
        kubectl(context, *drain, timeout=args.timeout + 30)
        print(f"==> Stopping {node} and changing CPU Manager to {args.policy}", flush=True)
        changed = True
        run("docker", "stop", "--time", "30", node, timeout=60)
        install_config(plan, plan["desired"])
        reset_checkpoint(plan)
        run("docker", "start", node)
        wait_for_config(context, node,
                        lambda cfg: matches(cfg, args.policy, plan["reserved"], not args.allow_smt_sharing), args.timeout)
    except (Exception, KeyboardInterrupt):
        # Stay cordoned unless recovery is verified; never schedule onto a broken node.
        if changed:
            print(f"==> Restoring previous CPU settings on {node}", file=sys.stderr, flush=True)
            try:
                show_logs(node)
            except Exception:
                pass
            try:
                run("docker", "stop", "--time", "30", node)
                install_config(plan, plan["previous"] or OWNER + "{}\n")
                reset_checkpoint(plan)
                run("docker", "start", node)
                wait_for_config(context, node, lambda cfg: signature(cfg) == signature(plan["before"]), args.timeout)
            except Exception as recovery_error:
                raise RuntimeError(f"Recovery failed for {node}: {recovery_error}. Node remains cordoned; inspect docker logs {node}.") from recovery_error
        if not plan["cordoned"]:
            kubectl(context, "uncordon", node)
        raise
    if not plan["cordoned"]:
        kubectl(context, "uncordon", node)
    print(f"==> {node}: verified policy={args.policy}" + (f", reserved CPUs={plan['reserved']}" if plan["reserved"] else ""), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Switch CPU Manager on existing k3d nodes. Drains pods and restarts node containers; preserves PVCs.")
    parser.add_argument("policy", choices=("static", "none"))
    parser.add_argument("--cluster", default="dimster", help="k3d cluster name (default: dimster)")
    parser.add_argument("--node", action="append", help="node container name; repeat to select nodes (default: all cluster nodes)")
    parser.add_argument("--reserved-cpus", default="auto", help="static: Linux CPU list, or auto to reserve one complete physical core")
    parser.add_argument("--allow-smt-sharing", action="store_true", help="static: set full-pcpus-only=false")
    parser.add_argument("--delete-emptydir-data", action="store_true", help="allow drain to discard pods' temporary emptyDir data")
    parser.add_argument("--timeout", type=int, default=300, help="seconds per drain/readiness phase (default: 300)")
    parser.add_argument("--dry-run", action="store_true", help="inspect configuration and print the plan without changing the cluster")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.policy == "none" and (args.reserved_cpus != "auto" or args.allow_smt_sharing):
        parser.error("--reserved-cpus and --allow-smt-sharing apply only to static mode")
    context = f"k3d-{args.cluster}"
    nodes = json.loads(kubectl(context, "get", "nodes", "-o", "json"))["items"]
    names = {node["metadata"]["name"] for node in nodes}
    if args.node and not set(args.node) <= names:
        parser.error("Unknown nodes: " + ", ".join(sorted(set(args.node) - names)))
    selected = [node for node in nodes if not args.node or node["metadata"]["name"] in args.node]
    if not selected:
        raise ValueError(f"No nodes found in {context}")
    # Preflight every selected node before draining any of them.
    plans = [preflight(context, node, args) for node in selected]
    for plan in plans:
        detail = "already configured" if plan["unchanged"] else "drain -> stop -> configure -> clear checkpoint -> start -> verify"
        print(f"{plan['node']}: {detail}", flush=True)
        if args.policy == "static":
            print(f"  full-pcpus-only={not args.allow_smt_sharing}, reserved CPUs={plan['reserved']}", flush=True)
    if args.dry_run:
        print("Dry run: no changes made.")
        return
    for plan in plans:
        transition(context, plan, args)
    print(f"Set dimster-config.yaml cpuManager.policy: {args.policy} and fullPcpusOnly: {str(args.policy == 'static' and not args.allow_smt_sharing).lower()}.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as error:
        sys.exit(f"ERROR: {error}")
