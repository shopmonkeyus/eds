#!/bin/bash
# PreToolUse hook (Bash): block an agent from cutting an EDS release.
#
# A release publishes signed binaries to a PUBLIC repo, notifies watchers on
# GitHub, and reaches customer-installed servers we cannot patch, restart, or
# roll back. EDS has no release workflow — releases run locally — so this hook
# is the only deterministic control that exists.
#
# Structure follows shopmonkeyus/backend .claude/hooks/block-push-main.sh:
# read the tool input JSON from stdin, normalize, split on shell separators,
# and inspect each segment on its own. That avoids false positives such as:
#   echo "git tag" && go test ./...
#   cd /tmp/release && go build
#
# Exit 2 = block; exit 0 = allow.
#
# Deliberately ALLOWED:
#   goreleaser release --snapshot   # builds locally, publishes nothing
#   git tag -l / --list             # reading tags
#   git tag -d / --delete           # local tag cleanup
#   git push origin <branch>        # ordinary branch pushes

cmd=$(jq -r '.tool_input.command // ""')

if [[ -z "${cmd}" ]]; then
  exit 0
fi

deny() {
  echo "BLOCKED: $1" >&2
  echo "Cutting an EDS release is a human action. See the Releasing section of CLAUDE.md." >&2
  exit 2
}

# Normalize: lowercase, collapse whitespace.
norm=$(printf '%s' "${cmd}" | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' ' ')

while IFS= read -r seg || [[ -n "${seg}" ]]; do
  seg="${seg# }"
  seg="${seg% }"

  # Strip leading env-var assignments so `GITHUB_TOKEN=x goreleaser release`
  # is still inspected.
  seg=$(printf '%s' "${seg}" | sed -E 's/^([A-Za-z_][A-Za-z_0-9]*=[^[:space:]]*[[:space:]]+)+//')

  # Strip git global options that sit between `git` and the subcommand, so
  # `git -c foo=bar push --tags` does not slip past.
  seg=$(printf '%s' "${seg}" | sed -E 's/^(git)( +(-[cC] +[^[:space:]]+|--(git-dir|work-tree|exec-path|namespace)=[^[:space:]]+|--no-pager))+ +/\1 /')

  # ---- goreleaser -------------------------------------------------------
  # Block `goreleaser release` unless it carries --snapshot, which builds
  # locally and publishes nothing.
  if [[ "${seg}" =~ ^goreleaser([[:space:]]|$) ]]; then
    if [[ "${seg}" =~ (^|[[:space:]])release([[:space:]]|$) ]] \
       && [[ ! "${seg}" =~ --snapshot([[:space:]]|=|$) ]]; then
      deny "goreleaser release publishes to the public repo."
    fi
  fi

  # ---- git tag ----------------------------------------------------------
  # Block tag CREATION. Allow list and delete.
  if [[ "${seg}" =~ ^git[[:space:]]+tag([[:space:]]|$) ]]; then
    if [[ ! "${seg}" =~ (^|[[:space:]])(-l|--list|-d|--delete|-n[0-9]*|--contains|--points-at|--verify|-v)([[:space:]]|=|$) ]]; then
      # A bare `git tag` just lists; anything with an argument creates.
      if [[ "${seg}" =~ ^git[[:space:]]+tag[[:space:]]+[^[:space:]] ]]; then
        deny "creating a release tag is the first step of a release."
      fi
    fi
  fi

  # ---- git push with tags ----------------------------------------------
  if [[ "${seg}" =~ ^git[[:space:]]+push([[:space:]]|$) ]]; then
    if [[ "${seg}" =~ (^|[[:space:]])--tags([[:space:]]|$) ]] \
       || [[ "${seg}" =~ (^|[[:space:]])--follow-tags([[:space:]]|$) ]] \
       || [[ "${seg}" =~ refs/tags/ ]] \
       || [[ "${seg}" =~ (^|[[:space:]])v[0-9]+\.[0-9]+ ]]; then
      deny "pushing a tag triggers the release."
    fi
  fi

  # ---- gh release -------------------------------------------------------
  if [[ "${seg}" =~ ^gh[[:space:]]+release([[:space:]]|$) ]]; then
    if [[ "${seg}" =~ (^|[[:space:]])(create|edit|upload|delete)([[:space:]]|$) ]]; then
      deny "gh release publishes to the public repo."
    fi
  fi

done < <(printf '%s\n' "${norm}" | tr ';|&' '\n')

exit 0
