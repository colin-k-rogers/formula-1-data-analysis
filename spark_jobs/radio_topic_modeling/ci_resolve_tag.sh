#!/usr/bin/env bash
# Given a branch name (arg 1), prints the ECR tag both
# .github/workflows/deploy_spark_staging.yaml and
# .github/workflows/cleanup_spark_staging.yaml use to identify that
# branch's staging image. Shared so the two workflows can never compute
# different tags for the same branch -- deploy pushes under whatever tag
# this prints, and cleanup deletes that exact tag; if that logic ever
# diverged between two separately-edited copies, cleanup would silently
# stop finding what deploy pushed, with no error.
set -euo pipefail
branch="$1"
echo "pr-$(echo "$branch" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+|-+$//g' | cut -c1-40)"
