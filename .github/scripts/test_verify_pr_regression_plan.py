#!/usr/bin/env python3
# Copyright © 2026 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
"""Unit tests for verify_pr_regression_plan.py and functions.sh JaCoCo hook."""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile
import textwrap
import unittest

import verify_pr_regression_plan as vprp

_VALID_PLAN = textwrap.dedent("""\
    ## Delta Regression Test Plan

    | Axis | Status | Scenario or Code/Config Evidence | Verification Oracle |
    | :--- | :--- | :--- | :--- |
    | Network Topology | `NOT_IMPACTED` | `cdap-common/bin/functions.sh` binds loopback `127.0.0.1` inside container only | Static `functions.sh` inspection |
    | Identity & Auth | `NOT_IMPACTED` | Launcher hook in `cdap-common/bin/functions.sh` does not modify auth tokens or RBAC | Static boundary check |
    | Edition & Storage/HA | `NOT_IMPACTED` | No `StructuredTable` or Spanner/Postgres SPI changes in `cdap-data-fabric` | Static SPI check |
    | Security & Data Governance | `IMPACTED` | Validate non-numeric `CDAP_JACOCO_PORT` values are rejected by `functions.sh` | Unit test asserts fake JVM receives zero `-javaagent` flags on injected port |
    | Action & Instance Options | `IMPACTED` | Toggle `CDAP_ENABLE_JACOCO_AGENT` between unset, `false`, and `true` | Unit test asserts `127.0.0.1:6300` flag iff env var is `true` and agent JAR exists |
    | Execution Surface | `IMPACTED` | Launch `MasterEnvironmentMain`, `TaskWorkerTwillRunnable`, `FileLocalizer`, and `ArtifactLocalizerTwillRunnable` | Primary service and task worker attach JaCoCo; localizers and exporter skip JaCoCo |
    | Lifecycle & Upgrade | `NOT_IMPACTED` | `UpgradeJobMain`, `PostUpgradeJobMain`, and `StorageMain` are excluded in `functions.sh` | Unit test asserts upgrade jobs receive zero `-javaagent` flags |
""")


class VerifyPrRegressionPlanTest(unittest.TestCase):

  def test_valid_seven_axis_plan_passes(self) -> None:
    self.assertEqual(vprp.validate_delta_test_plan(_VALID_PLAN), [])

  def test_generics_escaped_pipes_and_inequalities_allowed_while_placeholders_fail(
      self,
  ) -> None:
    generics_plan = _VALID_PLAN.replace(
        "No `StructuredTable` or Spanner/Postgres SPI changes in"
        " `cdap-data-fabric`",
        r"No `Map<String, String>` or `TaskWorker \| SystemWorker` SPI"
        " changes in `cdap-data-fabric`",
    ).replace(
        "Static SPI check",
        "Assert rows > 0 and errors < 1",
    )
    self.assertEqual(vprp.validate_delta_test_plan(generics_plan), [])

    placeholder_plan = _VALID_PLAN.replace(
        "Static SPI check",
        "<Exact assertion: sink rows, HTTP status, or mvn test>",
    )
    errors = vprp.validate_delta_test_plan(placeholder_plan)
    self.assertTrue(
        any("template placeholder" in err for err in errors),
        msg=f"Expected placeholder error, got {errors}",
    )

  def test_escaped_pipe_cannot_hide_vacuous_oracle_in_fourth_column(
      self,
  ) -> None:
    escaped_pipe_vacuous = _VALID_PLAN.replace(
        "Primary service and task worker attach JaCoCo; localizers and"
        " exporter skip JaCoCo",
        "HTTP 200",
    ).replace(
        "Launch `MasterEnvironmentMain`, `TaskWorkerTwillRunnable`,"
        " `FileLocalizer`, and `ArtifactLocalizerTwillRunnable`",
        r"Launch `TaskWorker \| SystemWorker` runnables with valid assertions",
    )
    errors = vprp.validate_delta_test_plan(escaped_pipe_vacuous)
    self.assertTrue(
        any("Execution Surface" in err and "vacuous" in err for err in errors),
        msg=f"Expected vacuous oracle rejection across escaped pipe: {errors}",
    )

  def test_missing_axis_rejected(self) -> None:
    lines = [
        line
        for line in _VALID_PLAN.splitlines()
        if "Lifecycle & Upgrade" not in line
    ]
    errors = vprp.validate_delta_test_plan("\n".join(lines))
    self.assertTrue(
        any("Lifecycle & Upgrade" in err for err in errors),
        msg=f"Expected missing axis error, got {errors}",
    )

  def test_not_impacted_without_code_citation_rejected(self) -> None:
    bad_plan = _VALID_PLAN.replace(
        "`cdap-common/bin/functions.sh` binds loopback `127.0.0.1` inside"
        " container only",
        "Not applicable to this pull request at all",
    )
    errors = vprp.validate_delta_test_plan(bad_plan)
    self.assertTrue(
        any(
            "Network Topology" in err and "NOT_IMPACTED" in err
            for err in errors
        ),
        msg=f"Expected NOT_IMPACTED citation error, got {errors}",
    )

  def test_vacuous_trash_sink_and_bare_http_200_rejected(self) -> None:
    trash_plan = _VALID_PLAN.replace(
        "Primary service and task worker attach JaCoCo; localizers and"
        " exporter skip JaCoCo",
        "Pipeline writes to Trash sink without errors",
    )
    errors = vprp.validate_delta_test_plan(trash_plan)
    self.assertTrue(
        any("Trash sink" in err for err in errors), msg=str(errors)
    )

    bare_200_plan = _VALID_PLAN.replace(
        "Primary service and task worker attach JaCoCo; localizers and"
        " exporter skip JaCoCo",
        "HTTP 200",
    )
    errors_200 = vprp.validate_delta_test_plan(bare_200_plan)
    self.assertTrue(
        any("Execution Surface" in err for err in errors_200),
        msg=str(errors_200),
    )

  def test_internal_leak_scanner_blocks_private_refs_and_commits(self) -> None:
    bug_ref = "b" + "/" + "123456789"
    cl_ref = "cl" + "/" + "987654321"
    go_ref = "go" + "/" + "internal-design-doc"
    corp_host = "test." + "corp." + "google.com"
    corp_proj = "google.com" + ":sample-dev"
    corp_email = "engineer@" + "google.com"

    leaky_diff = textwrap.dedent(f"""\
        diff --git a/cdap-app-fabric/src/main/java/Foo.java b/cdap-app-fabric/src/main/java/Foo.java
        --- a/cdap-app-fabric/src/main/java/Foo.java
        +++ b/cdap-app-fabric/src/main/java/Foo.java
        @@ -1,3 +1,8 @@
        +// Tracks {bug_ref} and {cl_ref}
        +// Design at {go_ref} and https://{corp_host}/doc
        +// Project {corp_proj} owner {corp_email}
        +// Standard Go package reference go/parser is public stdlib
    """)
    findings = vprp.scan_for_internal_leaks(
        leaky_diff,
        extra_texts={"<pr_title>": f"Fix issue ({bug_ref})"},
        commit_messages=(
            f"Subject line\n\nBody mentions {cl_ref}\n"
            f"Signed-off-by: Dev <{corp_email}>\n"
        ),
    )
    self.assertEqual(len(findings), 8, msg=f"Unexpected findings: {findings}")
    self.assertFalse(any("go/parser" in f for f in findings))

  def test_requires_delta_test_plan_and_quoted_diff_headers(self) -> None:
    self.assertFalse(
        vprp.requires_delta_test_plan([
            ".github/workflows/pr-regression-plan.yml",
            "specs/TEMPLATE.md",
            ".gemini/styleguide.md",
            "cdap-app-fabric/src/test/java/io/cdap/FooTest.java",
        ])
    )
    quoted_diff = (
        'diff --git "a/cdap-common/bin/my script.sh"'
        ' "b/cdap-common/bin/my script.sh"\n'
        '--- "a/cdap-common/bin/my script.sh"\n'
        '+++ "b/cdap-common/bin/my script.sh"\n'
    )
    extracted = vprp.extract_changed_files_from_diff(quoted_diff)
    self.assertEqual(extracted, ["cdap-common/bin/my script.sh"])
    self.assertTrue(vprp.requires_delta_test_plan(extracted))

  def test_unsafe_diff_path_traversal_rejected(self) -> None:
    bad_diff = (
        "diff --git a/../outside.txt b/../outside.txt\n"
        "--- a/../outside.txt\n"
        "+++ b/../outside.txt\n"
    )
    with self.assertRaises(ValueError):
      vprp.extract_changed_files_from_diff(bad_diff)

  def test_functions_sh_jacoco_hook_live_execution(self) -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    functions_sh = repo_root / "cdap-common" / "bin" / "functions.sh"
    self.assertTrue(functions_sh.is_file(), msg=f"Missing {functions_sh}")

    with tempfile.TemporaryDirectory() as tmp_str:
      tmp_dir = pathlib.Path(tmp_str)
      cdap_home = tmp_dir / "cdap"
      (cdap_home / "master" / "ext" / "jacoco").mkdir(parents=True)
      (cdap_home / "master" / "lib").mkdir(parents=True)
      (cdap_home / "conf").mkdir(parents=True)
      agent_jar = cdap_home / "master" / "ext" / "jacoco" / "jacocoagent.jar"
      agent_jar.write_bytes(b"fake-agent")
      (cdap_home / "master" / "lib" / "base.jar").write_bytes(b"fake-lib")

      java_home = tmp_dir / "java_home"
      (java_home / "bin").mkdir(parents=True)
      captured_args = tmp_dir / "java_args.txt"
      fake_java = java_home / "bin" / "java"
      fake_java.write_text(
          textwrap.dedent(f"""\
              #!/bin/bash
              if [[ "${{1:-}}" == "-version" ]]; then
                echo 'openjdk version "11.0.20"' >&2
                exit 0
              fi
              printf '%s\\n' "$@" > "{captured_args}"
              exit 0
          """),
          encoding="utf-8",
      )
      fake_java.chmod(0o755)

      def invoke_launcher(
          fn_name: str,
          main_class: str,
          extra_args: str = "",
          extra_env: dict[str, str] | None = None,
      ) -> list[str]:
        if captured_args.exists():
          captured_args.unlink()
        env = dict(os.environ)
        env.pop("CDAP_ENABLE_JACOCO_AGENT", None)
        env.pop("CDAP_JACOCO_PORT", None)
        env.pop("CDAP_JACOCO_DEST_DIR", None)
        env["JAVA_HOME"] = str(java_home)
        env["CDAP_HOME"] = str(cdap_home)
        env["CDAP_CONF"] = str(cdap_home / "conf")
        env["LOCAL_DIR"] = str(tmp_dir / "local")
        env["TEMP_DIR"] = str(tmp_dir / "tmp")
        (tmp_dir / "tmp").mkdir(exist_ok=True)
        if extra_env:
          env.update(extra_env)

        cmd = (
            f'source "{functions_sh}";'
            f' (set -u; cdap_set_jacoco_args "{main_class}" "{extra_args}");'
            f" {fn_name} {main_class} {extra_args}"
        )
        proc = subprocess.run(
            ["bash", "-c", cmd],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            msg=(
                f"functions.sh failed: stdout={proc.stdout}"
                f" stderr={proc.stderr}"
            ),
        )
        return captured_args.read_text(encoding="utf-8").splitlines()

      expected_flag = (
          f"-javaagent:{agent_jar}=output=tcpserver,address=127.0.0.1,"
          "port=6300,includes=io.cdap.*,inclnolocationclasses=true"
      )

      # 1. Unset env under `set -u`: clean execution, no agent attached.
      unset_args = invoke_launcher(
          "cdap_run_class",
          "io.cdap.cdap.master.environment.MasterEnvironmentMain",
      )
      self.assertFalse(any("jacocoagent.jar" in a for a in unset_args))

      # 2. Enabled on primary CDAP service across both cdap_run_class and
      # cdap_exec_class (used by `cdap run`): attaches 127.0.0.1:6300 agent.
      for fn_name in ("cdap_run_class", "cdap_exec_class"):
        service_args = invoke_launcher(
            fn_name,
            "io.cdap.cdap.master.environment.MasterEnvironmentMain",
            extra_env={"CDAP_ENABLE_JACOCO_AGENT": "true"},
        )
        self.assertIn(expected_flag, service_args)

      # 3. Enabled on KubeTwillLauncher TaskWorkerTwillRunnable: attaches agent.
      worker_args = invoke_launcher(
          "cdap_exec_class",
          "org.apache.twill.launcher.KubeTwillLauncher",
          "io.cdap.cdap.internal.app.worker.TaskWorkerTwillRunnable",
          extra_env={"CDAP_ENABLE_JACOCO_AGENT": "true"},
      )
      self.assertIn(expected_flag, worker_args)

      # 3b. Enabled on single-run PreviewRunnerTwillRunnable: uses persistent
      # file output (append=true) so preview.max.runs=1 stays unchanged, and
      # falls back safely if CDAP_JACOCO_DEST_DIR contains a comma.
      preview_dir = tmp_dir / "preview_pvc"
      preview_dir.mkdir(exist_ok=True)
      preview_args = invoke_launcher(
          "cdap_exec_class",
          "org.apache.twill.launcher.KubeTwillLauncher",
          "io.cdap.cdap.app.preview.PreviewRunnerTwillRunnable",
          extra_env={
              "CDAP_ENABLE_JACOCO_AGENT": "true",
              "CDAP_JACOCO_DEST_DIR": str(preview_dir),
          },
      )
      self.assertIn(
          f"-javaagent:{agent_jar}=output=file,"
          f"destfile={preview_dir}/jacoco-exit.exec,"
          "append=true,includes=io.cdap.*,inclnolocationclasses=true",
          preview_args,
      )
      comma_preview_args = invoke_launcher(
          "cdap_exec_class",
          "org.apache.twill.launcher.KubeTwillLauncher",
          "io.cdap.cdap.app.preview.PreviewRunnerTwillRunnable",
          extra_env={
              "CDAP_ENABLE_JACOCO_AGENT": "true",
              "CDAP_JACOCO_DEST_DIR": "/tmp/bad,injected=true",
          },
      )
      self.assertIn(
          f"-javaagent:{agent_jar}=output=file,"
          "destfile=/var/tmp/cdap/jacoco-exit.exec,"
          "append=true,includes=io.cdap.*,inclnolocationclasses=true",
          comma_preview_args,
      )

      # 4. Sidecar/init/localizer exclusions across both launcher shapes.
      for excluded_class, excluded_arg in [
          (
              "io.cdap.cdap.internal.app.runtime.k8s.FileLocalizer",
              "io.cdap.cdap.internal.app.worker.TaskWorkerTwillRunnable",
          ),
          (
              "org.apache.twill.launcher.KubeTwillLauncher",
              "io.cdap.cdap.internal.app.worker.sidecar."
              "ArtifactLocalizerTwillRunnable",
          ),
          ("io.cdap.cdap.metrics.process.SystemMetricsExporterServiceMain", ""),
          ("io.cdap.cdap.master.upgrade.UpgradeJobMain", ""),
          ("io.cdap.cdap.data.runtime.main.MasterStartupTool", ""),
      ]:
        got_args = invoke_launcher(
            "cdap_exec_class",
            excluded_class,
            excluded_arg,
            extra_env={"CDAP_ENABLE_JACOCO_AGENT": "true"},
        )
        self.assertFalse(
            any("jacocoagent.jar" in a for a in got_args),
            msg=(
                f"Expected exclusion for {excluded_class} {excluded_arg},"
                f" got {got_args}"
            ),
        )

      # 5. Non-numeric or out-of-range TCP ports rejected fail-closed.
      for bad_port in ("6300,destfile=/tmp/injected.exec", "0", "99999"):
        bad_port_args = invoke_launcher(
            "cdap_exec_class",
            "io.cdap.cdap.master.environment.MasterEnvironmentMain",
            extra_env={
                "CDAP_ENABLE_JACOCO_AGENT": "true",
                "CDAP_JACOCO_PORT": bad_port,
            },
        )
        self.assertFalse(any("jacocoagent.jar" in a for a in bad_port_args))


if __name__ == "__main__":
  unittest.main()
