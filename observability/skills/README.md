<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
# KeInFS Observability — Claude Code Skills

Claude Code skills that teach Claude how to observe, query, and troubleshoot a
KeInFS cluster's metrics. Each skill is a directory with a `SKILL.md`
(YAML frontmatter `name` + `description`, then markdown instructions). The
`description` is what Claude matches against a request, so they are trigger-rich.

## The skills

| Skill | What it does |
|-------|--------------|
| [`keinfs-observability`](keinfs-observability/SKILL.md) | **Entry point.** Where KeInFS metrics live (runtime stat trees `/run/keinfs/<svc>/<id>` + the `keinexport` :9909 sidecar + Prometheus :9090), the 6 Grafana dashboards by UID, quick health PromQL, and how to deploy Prometheus + Grafana (Docker `docker-compose.yml` or `install-baremetal.sh`). Routes to the two below. |
| [`keinfs-io-lifecycle-debug`](keinfs-io-lifecycle-debug/SKILL.md) | **Slow-I/O troubleshooting (flagship).** Encodes the `poc/IO_LIFECYCLE.md` phase map: given a symptom (slow writes / slow reads / low throughput / placement stall / KIX suspect / rebuild slow), which phase to inspect, what it means, what healthy looks like, and the exact PromQL / stat-file command to pull it. |
| [`keinfs-metrics-reference`](keinfs-metrics-reference/SKILL.md) | **Metric catalog.** Every `keinfs_*` family, its type, labels, and meaning, grouped by service (KST/KIX/KMS/KAS/KRS/exporter), plus a PromQL cookbook. Pure lookup reference. |

## Installing

Claude Code discovers skills in two locations:

- **User-level (all projects):** `~/.claude/skills/<skill-name>/SKILL.md`
- **Project-level (this repo only):** `<repo>/.claude/skills/<skill-name>/SKILL.md`

Symlink (recommended — stays in sync with the repo) or copy each skill dir:

```bash
# Project-scoped (only when working inside this repo):
mkdir -p .claude/skills
ln -s "$PWD/observability/skills/keinfs-observability"        .claude/skills/
ln -s "$PWD/observability/skills/keinfs-io-lifecycle-debug"   .claude/skills/
ln -s "$PWD/observability/skills/keinfs-metrics-reference"    .claude/skills/

# User-scoped (available everywhere):
mkdir -p ~/.claude/skills
for s in keinfs-observability keinfs-io-lifecycle-debug keinfs-metrics-reference; do
  ln -s "$PWD/observability/skills/$s" ~/.claude/skills/
done
```

Claude picks up the skill automatically when a request matches its
`description`. Start a request with the umbrella skill
(`keinfs-observability`) for anything cluster-observation related; it points at
the other two.

## Provenance

Metric and phase names in these skills are taken from the live KeInFS exporter
surface and `poc/IO_LIFECYCLE.md`; they are real, not invented. Dashboard UIDs
match `observability/grafana/dashboards/`: `keinfs-io-lifecycle`,
`keinfs-io-drilldown`, `keinfs-kst-overview`, `keinfs-kst-detail`,
`keinfs-kix-overview`, `keinfs-kix-detail`.
