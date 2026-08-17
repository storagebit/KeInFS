#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2026 Andreas Krause / storagebit
#
# Install the KeInFS git hooks by pointing core.hooksPath at this tracked dir.
# Idempotent. Run once per clone:  ./tools/git-hooks/install.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(git -C "$here" rev-parse --show-toplevel)"
rel="$(realpath --relative-to="$repo_root" "$here" 2>/dev/null || echo "tools/git-hooks")"

chmod +x "$here/pre-commit"
git -C "$repo_root" config core.hooksPath "$rel"
echo "core.hooksPath -> $rel"
echo "Installed KeInFS pre-commit secret/infra guard."
echo "Test it:  git commit  (it scans staged changes; --no-verify bypasses)."
