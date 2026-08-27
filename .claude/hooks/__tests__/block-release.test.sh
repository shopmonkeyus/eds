#!/bin/bash
# Tests for block-release.sh.
#
# Run: ./.claude/hooks/__tests__/block-release.test.sh
# Exit 0 = all pass.

HOOK="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/block-release.sh"
pass=0
fail=0

# run <expected: block|allow> <description> <command>
run() {
  local expect="$1" desc="$2" cmd="$3"
  local out rc
  out=$(printf '%s' "{\"tool_input\":{\"command\":$(printf '%s' "$cmd" | jq -Rs .)}}" | bash "$HOOK" 2>&1)
  rc=$?
  local got="allow"
  [[ $rc -eq 2 ]] && got="block"
  if [[ "$got" == "$expect" ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    printf '  FAIL  expected=%-5s got=%-5s  %s\n        cmd: %s\n' "$expect" "$got" "$desc" "$cmd"
    [[ -n "$out" ]] && printf '        out: %s\n' "$(printf '%s' "$out" | head -1)"
  fi
}

# ---- must block ---------------------------------------------------------
run block "goreleaser release"                 'goreleaser release --clean'
run block "goreleaser release, no flags"       'goreleaser release'
run block "goreleaser with env prefix"         'GITHUB_TOKEN=abc goreleaser release --clean'
run block "annotated tag"                      'git tag -a v2.0.2 -m "release"'
run block "lightweight tag"                    'git tag v2.0.2'
run block "push --tags"                        'git push --tags'
run block "push --follow-tags"                 'git push origin --follow-tags'
run block "push explicit tag ref"              'git push origin refs/tags/v2.0.2'
run block "push a version refspec"             'git push origin v2.0.2'
run block "push with git -c smuggling"         'git -c http.proxy=x push origin --tags'
run block "gh release create"                  'gh release create v2.0.2 --notes "x"'
run block "gh release upload"                  'gh release upload v2.0.2 ./dist/eds'
run block "chained after a safe command"       'go build ./... && goreleaser release --clean'
run block "tag then push, second segment"      'echo ok; git push --tags'

# ---- must allow ---------------------------------------------------------
run allow "snapshot build"                     'goreleaser release --snapshot --clean'
run allow "snapshot, flag order swapped"       'goreleaser release --clean --snapshot'
run allow "goreleaser check"                   'goreleaser check'
run allow "goreleaser build"                   'goreleaser build --single-target'
run allow "list tags"                          'git tag -l'
run allow "list tags long flag"                'git tag --list'
run allow "bare git tag lists"                 'git tag'
run allow "delete a local tag"                 'git tag -d v2.0.2'
run allow "ordinary branch push"               'git push origin add-claude-md'
run allow "push to a feature branch"           'git push -u origin feature/eds-driver'
run allow "gh pr create"                       'gh pr create --title x --body y'
run allow "gh release list"                    'gh release list'
run allow "go test"                            'go test ./...'
run allow "the word release in a path"         'cd internal/release && go build'
run allow "echo mentioning goreleaser release" 'echo "run goreleaser release to ship"'

printf '\n  %s passed, %s failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]] || exit 1
