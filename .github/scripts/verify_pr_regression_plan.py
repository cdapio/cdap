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
"""Validates CDAP PR 7-axis Delta Test Plans and non-vacuity gates."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import os
import pathlib
import re
import subprocess
import sys

CANONICAL_AXES: tuple[str, ...] = (
    "Network Topology",
    "Identity & Auth",
    "Edition & Storage/HA",
    "Security & Data Governance",
    "Action & Instance Options",
    "Execution Surface",
    "Lifecycle & Upgrade",
)

_GO_STDLIB_PACKAGES: frozenset[str] = frozenset({
    "ast",
    "build",
    "constant",
    "doc",
    "format",
    "importer",
    "parser",
    "printer",
    "scanner",
    "token",
    "types",
    "version",
})

# Constructed from token parts so static scans of this repository never flag the
# validator source file itself as containing private shortlinks or hostnames.
_BUG_SHORTLINK_RE = re.compile(r"(?<![A-Za-z0-9_/.-])" + "b" + r"/\d{5,}\b")
_CL_SHORTLINK_RE = re.compile(r"(?<![A-Za-z0-9_/.-])" + "cl" + r"/\d{5,}\b")
_GO_SHORTLINK_RE = re.compile(
    r"(?<![A-Za-z0-9_/.-])" + "go" + r"/([a-z0-9][a-z0-9_-]{1,})\b"
)
_CORP_HOST_RE = re.compile(
    r"\b[a-z0-9.-]*(?:"
    + "corp"
    + r"\."
    + "google"
    + r"\.com|"
    + "googleplex"
    + r"\.com)\b",
    re.IGNORECASE,
)
_INTERNAL_PROJECT_RE = re.compile(
    r"\b" + "google" + r"\.com:[a-z0-9-]+\b", re.IGNORECASE
)
_INTERNAL_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@" + "google" + r"\.com\b", re.IGNORECASE
)
_GIT_TRAILER_RE = re.compile(
    r"^\s*(?:Co-authored-by|Signed-off-by|Reviewed-by):", re.IGNORECASE
)

_DIFF_GIT_HEADER_RE = re.compile(r'^diff --git "?a/(.+?)"? "?b/(.+?)"?$')
_PLUS_FILE_HEADER_RE = re.compile(r'^\+\+\+ "?b/(.+?)"?$')
_TEMPLATE_PLACEHOLDER_RE = re.compile(
    r"<\s*(?:e\.g\.|cite\b|list\b|exact\b|mvn\b|file:line\b|"
    r"scenario\b|oracle\b|todo\b)[^>]*>",
    re.IGNORECASE,
)
_EVIDENCE_ANCHOR_RE = re.compile(
    r"(/|\.java\b|\.scala\b|\.py\b|\.sh\b|\.xml\b|\.yaml\b|\.yml\b|"
    r"\bcdap-[a-z0-9-]+\b|\bcConf\b|\bSPI\b|\bStructuredTable\b|"
    r"\bTwillRunnable\b|\bcdap-default\.xml\b|\bcdap-site\.xml\b)"
)
_HANDWAVY_ONLY_RE = re.compile(
    r"^(?:n/?a|none|unrelated|not\s+applicable|no\s+impact|no\s+change|"
    r"unchanged|same\s+as\s+before)[.!\s]*$",
    re.IGNORECASE,
)
_TRASH_SINK_RE = re.compile(r"\btrash\s+sink\b|\bsink\s*[:=]\s*trash\b", re.I)
_BARE_STATUS_ONLY_RE = re.compile(
    r"^(?:returns?\s+)?(?:http\s*200|status\s*200|200\s*ok|completed|"
    r"status\s*==?\s*['\"]?completed['\"]?)[.!\s]*$",
    re.IGNORECASE,
)


def _is_safe_rel_path(path_str: str) -> bool:
  if not path_str or path_str.startswith("/"):
    return False
  parts = pathlib.PurePosixPath(path_str).parts
  return ".." not in parts


def extract_changed_files_from_diff(diff_text: str) -> list[str]:
  """Extracts unique repository-relative file paths from a unified git diff."""
  seen: dict[str, None] = {}
  for line in diff_text.splitlines():
    git_match = _DIFF_GIT_HEADER_RE.match(line)
    if git_match:
      candidate = git_match.group(2)
      if not _is_safe_rel_path(candidate):
        raise ValueError(f"Unsafe path in diff header: {candidate!r}")
      seen[candidate] = None
      continue
    plus_match = _PLUS_FILE_HEADER_RE.match(line.strip())
    if plus_match:
      candidate = plus_match.group(1)
      if not _is_safe_rel_path(candidate):
        raise ValueError(f"Unsafe path in +++ header: {candidate!r}")
      seen[candidate] = None
  return list(seen.keys())


def is_production_runtime_path(rel_path: str) -> bool:
  """Returns True when rel_path modifies production CDAP runtime code/config."""
  norm = rel_path.strip("/")
  if (
      norm.startswith(".github/")
      or norm.startswith(".gemini/")
      or norm.startswith("specs/")
      or norm.endswith(".md")
  ):
    return False
  if "/src/test/" in f"/{norm}" or norm.endswith("Test.java"):
    return False
  return (
      "/src/main/" in f"/{norm}"
      or "/bin/" in f"/{norm}"
      or "/conf/" in f"/{norm}"
  )


def requires_delta_test_plan(changed_files: Sequence[str]) -> bool:
  """Returns True if any touched path requires a 7-axis Delta Test Plan."""
  return any(is_production_runtime_path(path) for path in changed_files)


def _scan_line_for_leaks(
    source_label: str,
    line_no: int,
    text: str,
    allow_git_trailers: bool = False,
) -> list[str]:
  """Checks one added line or document line for non-public identifiers."""
  findings: list[str] = []
  if _BUG_SHORTLINK_RE.search(text):
    findings.append(
        f"{source_label}:{line_no}: private bug tracker shortlink detected"
    )
  if _CL_SHORTLINK_RE.search(text):
    findings.append(
        f"{source_label}:{line_no}: private changelist shortlink detected"
    )
  for match in _GO_SHORTLINK_RE.finditer(text):
    slug = match.group(1)
    if slug not in _GO_STDLIB_PACKAGES:
      findings.append(
          f"{source_label}:{line_no}: private shortlink 'go/{slug}' detected"
      )
  if _CORP_HOST_RE.search(text):
    findings.append(
        f"{source_label}:{line_no}: internal corporate hostname detected"
    )
  if _INTERNAL_PROJECT_RE.search(text):
    findings.append(
        f"{source_label}:{line_no}: internal domain-scoped GCP project detected"
    )
  if _INTERNAL_EMAIL_RE.search(text) and not (
      allow_git_trailers and _GIT_TRAILER_RE.match(text)
  ):
    findings.append(
        f"{source_label}:{line_no}: internal corporate email address detected"
    )
  return findings


def scan_for_internal_leaks(
    diff_text: str,
    extra_texts: Mapping[str, str] | None = None,
    commit_messages: str = "",
) -> list[str]:
  """Scans added diff lines, commit messages, and PR texts for non-public refs."""
  findings: list[str] = []
  current_file = "<diff>"
  added_line_no = 0
  for raw_line in diff_text.splitlines():
    plus_match = _PLUS_FILE_HEADER_RE.match(raw_line.strip())
    if plus_match:
      current_file = plus_match.group(1)
      added_line_no = 0
      continue
    if raw_line.startswith("@@ "):
      hunk_match = re.search(r"\+(\d+)", raw_line)
      added_line_no = int(hunk_match.group(1)) if hunk_match else 0
      continue
    if raw_line.startswith("+") and not raw_line.startswith("+++"):
      findings.extend(
          _scan_line_for_leaks(current_file, added_line_no, raw_line[1:])
      )
      added_line_no += 1
    elif not raw_line.startswith("-"):
      added_line_no += 1

  if extra_texts:
    for label, content in extra_texts.items():
      for idx, line in enumerate(content.splitlines(), start=1):
        findings.extend(_scan_line_for_leaks(label, idx, line))

  if commit_messages:
    for idx, line in enumerate(commit_messages.splitlines(), start=1):
      findings.extend(
          _scan_line_for_leaks(
              "<commit_messages>", idx, line, allow_git_trailers=True
          )
      )
  return findings


def _strip_cell(cell: str) -> str:
  cleaned = cell.strip()
  if cleaned.startswith("`") and cleaned.endswith("`") and len(cleaned) >= 2:
    cleaned = cleaned[1:-1].strip()
  cleaned = re.sub(r"^\*\*|\*\*$", "", cleaned).strip()
  return cleaned


def validate_delta_test_plan(plan_text: str) -> list[str]:
  """Validates a Markdown 7-Axis Orthogonality Matrix Delta Test Plan."""
  errors: list[str] = []
  if not plan_text or not plan_text.strip():
    return ["Missing 7-axis Delta Regression Test Plan."]

  canonical_by_lower = {axis.lower(): axis for axis in CANONICAL_AXES}
  seen_axes: dict[str, tuple[str, str, str]] = {}

  for line in plan_text.splitlines():
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
      continue
    cells = [
        c.replace(r"\|", "|").strip()
        for c in re.split(r"(?<!\\)\|", stripped[1:-1])
    ]
    if len(cells) < 4:
      continue
    axis_raw = _strip_cell(cells[0])
    if not axis_raw or set(axis_raw) <= {":", "-", " "}:
      continue
    canonical = canonical_by_lower.get(axis_raw.lower())
    if canonical is None:
      continue
    status = _strip_cell(cells[1]).upper()
    evidence = cells[2].strip()
    oracle = cells[3].strip()
    seen_axes[canonical] = (status, evidence, oracle)

  for axis in CANONICAL_AXES:
    if axis not in seen_axes:
      errors.append(f"Missing required orthogonality axis row: '{axis}'.")
      continue
    status, evidence, oracle = seen_axes[axis]
    if status not in {"IMPACTED", "NOT_IMPACTED"}:
      errors.append(
          f"Axis '{axis}' has invalid status '{status}'; expected IMPACTED or"
          " NOT_IMPACTED."
      )
      continue
    if _TEMPLATE_PLACEHOLDER_RE.search(evidence):
      errors.append(
          f"Axis '{axis}' still contains unedited template placeholder in"
          " scenario/evidence."
      )
    if _TEMPLATE_PLACEHOLDER_RE.search(oracle):
      errors.append(
          f"Axis '{axis}' still contains unedited template placeholder in"
          " verification oracle."
      )

    if status == "NOT_IMPACTED":
      plain_evidence = _strip_cell(evidence)
      if (
          len(plain_evidence) < 15
          or _HANDWAVY_ONLY_RE.match(plain_evidence)
          or not _EVIDENCE_ANCHOR_RE.search(evidence)
      ):
        errors.append(
            f"Axis '{axis}' is marked NOT_IMPACTED without concrete file,"
            " module, SPI, or config-key citation."
        )
    else:
      plain_scenario = _strip_cell(evidence)
      plain_oracle = _strip_cell(oracle)
      if len(plain_scenario) < 15:
        errors.append(
            f"Axis '{axis}' is marked IMPACTED but scenario description is too"
            " brief (< 15 chars)."
        )
      if len(plain_oracle) < 10:
        errors.append(
            f"Axis '{axis}' is marked IMPACTED but verification oracle is too"
            " brief (< 10 chars)."
        )
      if _TRASH_SINK_RE.search(oracle) or _TRASH_SINK_RE.search(evidence):
        errors.append(
            f"Axis '{axis}' uses a vacuous Trash sink; assert a real sink or"
            " persisted state transition instead."
        )
      if _BARE_STATUS_ONLY_RE.match(plain_oracle):
        errors.append(
            f"Axis '{axis}' oracle '{plain_oracle}' is vacuous; assert output"
            " rows, persisted state, or exact error status + message."
        )

  return errors


def _load_git_diff(git_range: str) -> str:
  proc = subprocess.run(
      ["git", "-c", "core.quotePath=false", "diff", "--unified=3", git_range],
      check=True,
      capture_output=True,
      text=True,
  )
  return proc.stdout


def _load_git_commit_messages(git_range: str) -> str:
  log_range = (
      git_range.replace("...", "..", 1) if "..." in git_range else git_range
  )
  proc = subprocess.run(
      ["git", "log", "--format=%B", log_range],
      check=True,
      capture_output=True,
      text=True,
  )
  return proc.stdout


def _discover_changed_spec_texts(
    repo_root: pathlib.Path, changed_files: Sequence[str]
) -> dict[str, str]:
  spec_texts: dict[str, str] = {}
  for rel_path in changed_files:
    if (
        rel_path.startswith("specs/")
        and rel_path.endswith(".md")
        and rel_path != "specs/TEMPLATE.md"
    ):
      candidate = (repo_root / rel_path).resolve()
      if not str(candidate).startswith(str(repo_root.resolve()) + os.sep):
        raise ValueError(f"Spec path escapes repository root: {rel_path}")
      if candidate.is_file():
        spec_texts[rel_path] = candidate.read_text(encoding="utf-8")
  return spec_texts


def main(argv: Sequence[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
      description="Validate CDAP PR 7-axis Delta Regression Test Plan."
  )
  parser.add_argument(
      "--diff-file",
      help="Path to unified diff file (alternative to --git-range).",
  )
  parser.add_argument(
      "--git-range",
      help="Git revision range to diff (for example origin/develop...HEAD).",
  )
  parser.add_argument(
      "--pr-body-file",
      help="Optional file containing pull request description markdown.",
  )
  parser.add_argument(
      "--repo-root",
      default=".",
      help="Repository root directory for resolving changed specs/*.md files.",
  )
  args = parser.parse_args(argv)

  if bool(args.diff_file) == bool(args.git_range):
    parser.error("Specify exactly one of --diff-file or --git-range.")

  commit_messages = ""
  if args.diff_file:
    diff_text = pathlib.Path(args.diff_file).read_text(encoding="utf-8")
  else:
    diff_text = _load_git_diff(args.git_range)
    commit_messages = _load_git_commit_messages(args.git_range)

  changed_files = extract_changed_files_from_diff(diff_text)
  repo_root = pathlib.Path(args.repo_root).resolve()
  plan_texts = _discover_changed_spec_texts(repo_root, changed_files)
  leak_texts = dict(plan_texts)

  pr_title = os.environ.get("PR_TITLE", "")
  if pr_title:
    leak_texts["<pr_title>"] = pr_title

  pr_body = os.environ.get("PR_BODY", "")
  if args.pr_body_file:
    pr_body = pathlib.Path(args.pr_body_file).read_text(encoding="utf-8")
  if pr_body:
    plan_texts["<pr_body>"] = pr_body
    leak_texts["<pr_body>"] = pr_body

  leak_errors = scan_for_internal_leaks(
      diff_text, leak_texts, commit_messages=commit_messages
  )
  plan_errors: list[str] = []
  if requires_delta_test_plan(changed_files):
    combined_plan_sources = "\n\n".join(plan_texts.values())
    plan_errors = validate_delta_test_plan(combined_plan_sources)

  all_errors = leak_errors + plan_errors
  if all_errors:
    for err in all_errors:
      print(f"[ERROR] {err}", file=sys.stderr)
    return 1

  print(
      "PR regression plan verification passed"
      f" ({len(changed_files)} changed files,"
      f" plan_required={requires_delta_test_plan(changed_files)})."
  )
  return 0


if __name__ == "__main__":
  sys.exit(main())
