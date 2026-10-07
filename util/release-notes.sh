#!/usr/bin/env bash
# Generate a Markdown release-note draft from two Git revisions.

set -euo pipefail

usage() {
    echo "Usage: $0 OLD_REF NEW_REF" >&2
    echo "Example: $0 v26.09.1 HEAD" >&2
}

if [[ $# -ne 2 ]]; then
    usage
    exit 2
fi

old_ref=$1
new_ref=$2

if ! git rev-parse --verify --quiet "${old_ref}^{commit}" >/dev/null;
then
    echo "Revision not found: ${old_ref}" >&2
    exit 2
fi

if ! git rev-parse --verify --quiet "${new_ref}^{commit}" >/dev/null;
then
    echo "Revision not found: ${new_ref}" >&2
    exit 2
fi

if ! git merge-base --is-ancestor "${old_ref}" "${new_ref}"; then
    echo "Revision is not an ancestor of ${new_ref}: ${old_ref}" >&2
    exit 2
fi

new_tag=$(git describe --exact-match --tags "${new_ref}" 2>/dev/null || true)
if [[ -n "${new_tag}" ]]; then
    release_version=${new_tag#v}
elif [[ "${new_ref}" == "HEAD" ]]; then
    release_version=$(sed -n 's/^__version__ = "\([^"]*\)"/\1/p' \
        dkit/__init__.py)
    if [[ -z "${release_version}" ]]; then
        echo "Could not read dkit.__version__ from dkit/__init__.py" >&2
        exit 2
    fi
else
    release_version=${new_ref}
fi

declare -a added=()
declare -a changed=()
declare -a fixed=()
declare -a documentation=()
declare -a tests=()
declare -a other=()

while IFS= read -r subject; do
    [[ -z "${subject}" ]] && continue

    case "${subject}" in
        feat\!:\ *|feat\(*\)\!:\ *)
            added+=("${subject#*: }")
            ;;
        feat:\ *|feat\(*\):\ *)
            added+=("${subject#*: }")
            ;;
        fix:\ *|fix\(*\):\ *)
            fixed+=("${subject#*: }")
            ;;
        docs:\ *|docs\(*\):\ *)
            documentation+=("${subject#*: }")
            ;;
        test:\ *|test\(*\):\ *)
            tests+=("${subject#*: }")
            ;;
        refactor:\ *|refactor\(*\):\ *|chore:\ *)
            changed+=("${subject#*: }")
            ;;
        *)
            other+=("${subject}")
            ;;
    esac
done < <(git log --no-merges --format='%s' "${old_ref}..${new_ref}")

print_section() {
    local heading=$1
    shift
    local entries=("$@")

    [[ ${#entries[@]} -eq 0 ]] && return

    echo "## ${heading}"
    echo
    for entry in "${entries[@]}"; do
        echo "- ${entry}"
    done
    echo
}

shortstat=$(git diff --shortstat "${old_ref}..${new_ref}")
file_count=$(git diff --name-only "${old_ref}..${new_ref}" | wc -l)

echo "# ${release_version}"
echo
print_section "Added" "${added[@]}"
print_section "Changed" "${changed[@]}"
print_section "Fixed" "${fixed[@]}"
print_section "Documentation" "${documentation[@]}"
print_section "Tests" "${tests[@]}"
print_section "Other" "${other[@]}"
echo "## Summary"
echo
echo "- ${file_count} files changed"
if [[ -n "${shortstat}" ]]; then
    echo "- ${shortstat#*, }"
fi
