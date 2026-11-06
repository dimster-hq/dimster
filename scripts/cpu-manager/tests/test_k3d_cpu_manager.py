"""Exercise topology and failure handling without needing a Docker daemon."""

import argparse
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


MODULE = Path(__file__).resolve().parents[1] / "lib" / "k3d_cpu_manager.py"
spec = importlib.util.spec_from_file_location("cpu_manager", MODULE)
cpu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpu)


class ConfigurationTests(unittest.TestCase):
    """Check hardware and configuration constraints before any node is drained."""

    def test_auto_reserves_siblings_not_adjacent_logical_cpus(self):
        reserved, width = cpu.select_reserved("0-7\n0,4\n1,5\n2,6\n3,7\n", "auto", True)
        self.assertEqual((reserved, width), ("0,4", 2))

    def test_auto_respects_container_cpu_affinity(self):
        self.assertEqual(cpu.select_reserved("1,3,5,7\n0,4\n1,5\n2,6\n3,7\n", "auto", True), ("1,5", 2))

    def test_invalid_reservations_are_rejected(self):
        for reserved in ("0", "0-7", "8", "3-1", "0;id"):
            with self.subTest(reserved=reserved), self.assertRaises(ValueError):
                cpu.select_reserved("0-7\n0,4\n1,5\n2,6\n3,7\n", reserved, True)

    def test_allow_smt_sharing_accepts_single_thread_reservation(self):
        self.assertEqual(cpu.select_reserved("0-7\n0,4\n1,5\n2,6\n3,7\n", "0", False), ("0", 2))

    def test_none_removes_helper_reservation_and_options(self):
        config = json.loads(cpu.render_config("none").removeprefix(cpu.OWNER))
        self.assertEqual(config, {"kubelet-arg+": ["cpu-manager-policy=none"]})

    def test_unrelated_config_is_accepted_without_rewriting(self):
        config = {"/etc/rancher/k3s/config.yaml": "kubelet-arg:\n  - eviction-hard=memory.available<100Mi\ndisable: [traefik]\n"}
        before = dict(config)
        cpu.validate_configuration({"Config": {"Cmd": ["server"], "Env": []}}, config)
        self.assertEqual(before, config)

    def test_conflicting_config_or_cli_is_rejected(self):
        cases = [
            ({"Config": {"Cmd": ["server", "--kubelet-arg=cpu-manager-policy=none"]}}, {}),
            ({"Config": {"Env": ["K3S_CONFIG_FILE=/custom.yaml"]}}, {}),
            ({"Config": {}}, {"/etc/rancher/k3s/config.yaml": "kubelet-arg: [reserved-cpus=0]"}),
            ({"Config": {}}, {cpu.CONFIG_PATH: "unowned: file"}),
            ({"Config": {}}, {cpu.CONFIG_DIR + "/zzz.yaml": "kubelet-arg: [v=1]"}),
        ]
        for container, configs in cases:
            with self.subTest(container=container, configs=configs), self.assertRaises(ValueError):
                cpu.validate_configuration(container, configs)

    def test_full_core_check_only_rejects_eligible_misaligned_containers(self):
        pods = []
        for name, qos, request in (("coordinator", "Guaranteed", "1"), ("broker", "Guaranteed", "4000m"),
                                   ("shared", "Guaranteed", "500m"), ("burstable", "Burstable", "1")):
            pods.append({"metadata": {"namespace": "dimster", "name": name},
                         "status": {"qosClass": qos, "phase": "Running"},
                         "spec": {"containers": [{"name": name, "resources": {"requests": {"cpu": request}}}]}})
        self.assertEqual(cpu.incompatible_pods(pods, 2), ["dimster/coordinator:coordinator requests 1 CPUs"])

    def test_full_core_check_includes_init_containers(self):
        pod = {"metadata": {"namespace": "test", "name": "init"}, "status": {"qosClass": "Guaranteed"},
               "spec": {"initContainers": [{"name": "init", "resources": {"requests": {"cpu": "3"}}}]}}
        self.assertEqual(len(cpu.incompatible_pods([pod], 2)), 1)
        pod["status"]["phase"] = "Succeeded"
        self.assertEqual(cpu.incompatible_pods([pod], 2), [])

    def test_checkpoint_reset_refuses_running_node(self):
        with patch.object(cpu, "run", return_value="true") as command:
            with self.assertRaisesRegex(RuntimeError, "while kubelet is running"):
                cpu.reset_checkpoint({"node": "test"})
            self.assertEqual(command.call_count, 1)


class TransitionTests(unittest.TestCase):
    """Verify shutdown ordering, cordon preservation, and rollback boundaries."""

    def setUp(self):
        self.plan = {"node": "test", "image": "sha256:test", "volume": "kubelet-volume",
                     "previous": None, "desired": cpu.render_config("static", "0,4"),
                     "before": {"cpuManagerPolicy": "none"}, "reserved": "0,4",
                     "cordoned": False, "unchanged": False}
        self.args = argparse.Namespace(policy="static", timeout=1, delete_emptydir_data=False, allow_smt_sharing=False)
        self.events = []
        for method in ("run", "kubectl", "install_config", "reset_checkpoint", "wait_for_config", "show_logs"):
            mock = patch.object(cpu, method, side_effect=lambda *a, _method=method, **kw: self.events.append((_method, a)) or "")
            setattr(self, method, mock.start())
            self.addCleanup(mock.stop)

    def test_success_resets_only_after_stop_and_uncordons_only_after_verification(self):
        cpu.transition("context", self.plan, self.args)
        self.assertEqual([method for method, _ in self.events],
                         ["kubectl", "run", "install_config", "reset_checkpoint", "run", "wait_for_config", "kubectl"])
        self.assertEqual(self.events[1][1][:2], ("docker", "stop"))
        self.assertEqual(self.events[4][1][:2], ("docker", "start"))
        self.assertEqual(self.events[-1][1], ("context", "uncordon", "test"))
        self.assertNotIn("--force", self.events[0][1])
        self.assertNotIn("--delete-emptydir-data", self.events[0][1])

    def test_existing_cordon_is_preserved(self):
        self.plan["cordoned"] = True
        cpu.transition("context", self.plan, self.args)
        self.assertEqual(self.kubectl.call_count, 1)

    def test_unchanged_policy_does_not_drain_or_restart(self):
        self.plan["unchanged"] = True
        cpu.transition("context", self.plan, self.args)
        self.assertEqual(self.events, [])

    def test_drain_failure_does_not_touch_node_config_or_checkpoint(self):
        self.kubectl.side_effect = [RuntimeError("PDB prevents eviction"), ""]
        with self.assertRaisesRegex(RuntimeError, "PDB"):
            cpu.transition("context", self.plan, self.args)
        self.run.assert_not_called()
        self.install_config.assert_not_called()
        self.reset_checkpoint.assert_not_called()
        self.assertEqual(self.kubectl.call_args.args, ("context", "uncordon", "test"))

    def test_failed_restart_restores_previous_config_and_verifies_it(self):
        original = cpu.render_config("static", "1,5", False)
        self.plan["previous"] = original
        self.wait_for_config.side_effect = [RuntimeError("wrong policy"), None]
        with self.assertRaisesRegex(RuntimeError, "wrong policy"):
            cpu.transition("context", self.plan, self.args)
        self.assertEqual(self.install_config.call_args.args[1], original)
        self.assertEqual(self.reset_checkpoint.call_count, 2)
        self.assertEqual(self.kubectl.call_args.args, ("context", "uncordon", "test"))

    def test_failed_recovery_leaves_node_cordoned(self):
        self.wait_for_config.side_effect = RuntimeError("kubelet unavailable")
        with self.assertRaisesRegex(RuntimeError, "Recovery failed"):
            cpu.transition("context", self.plan, self.args)
        self.assertEqual(self.kubectl.call_count, 1)


class MainTests(unittest.TestCase):
    """Enforce the dry-run and preflight-before-mutation interface contracts."""

    def test_dry_run_does_not_transition(self):
        nodes = {"items": [{"metadata": {"name": "node"}}]}
        plan = {"node": "node", "unchanged": False, "reserved": "0,4"}
        with patch.object(cpu, "kubectl", return_value=json.dumps(nodes)), patch.object(cpu, "preflight", return_value=plan), patch.object(cpu, "transition") as transition:
            cpu.main(["static", "--dry-run"])
            transition.assert_not_called()

    def test_bad_second_node_prevents_changes_to_first(self):
        nodes = {"items": [{"metadata": {"name": name}} for name in ("one", "two")]}
        with patch.object(cpu, "kubectl", return_value=json.dumps(nodes)), patch.object(cpu, "preflight", side_effect=[{}, ValueError("conflict")]), patch.object(cpu, "transition") as transition:
            with self.assertRaisesRegex(ValueError, "conflict"):
                cpu.main(["static"])
            transition.assert_not_called()


if __name__ == "__main__":
    unittest.main()
