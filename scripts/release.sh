#!/bin/sh
#
# Release a version: set it in pyproject.toml and uv.lock, commit that alone
# on main, and put the annotated tag vX.Y.Z on the same commit.
#
#     make release VERSION=X.Y.Z
#
# Pushing a vX.Y.Z tag builds and publishes the container image, and the image
# workflow refuses a tag that differs from the version in pyproject.toml. Doing
# the bump, the commit and the tag in one step keeps the tag on the commit that
# carries its version, with the v prefix the workflow needs. Nothing is pushed:
# the script prints the two push commands for you to run.

set -eu

fail() {
    echo "release: $*" >&2
    exit 1
}

version=${1:-}
if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "usage: make release VERSION=X.Y.Z (digits only, no leading v)" >&2
    exit 2
fi
tag="v$version"

[ "$(git rev-parse --abbrev-ref HEAD)" = main ] ||
    fail "releases are made on main; switch to it first"
[ -z "$(git status --porcelain --untracked-files=no)" ] ||
    fail "the working tree has uncommitted changes; commit or stash them first"

git fetch --quiet origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse FETCH_HEAD)" ] ||
    fail "main differs from origin/main; pull or push first, so the release holds exactly what is merged"

git rev-parse --quiet --verify "refs/tags/$tag" >/dev/null &&
    fail "the tag $tag already exists here"
git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null &&
    fail "the tag $tag already exists on origin"

current=$(uv version --short --offline)
[ "$current" != "$version" ] ||
    fail "pyproject.toml already says $version"

uv version "$version" --no-sync --offline >/dev/null

changed=$(git diff --name-only | tr '\n' ' ')
if [ "$changed" != "pyproject.toml uv.lock " ]; then
    echo "release: expected pyproject.toml and uv.lock to change, got: $changed" >&2
    echo "Inspect with git diff, and undo with: git checkout -- pyproject.toml uv.lock" >&2
    exit 1
fi

git add pyproject.toml uv.lock
git commit --quiet -m "chore: bump the version to $version"
git tag -a "$tag" -m "$tag"

echo "Released $current -> $version as $tag on $(git rev-parse --short HEAD)."
echo "Check it with git show $tag, then push the commit and the tag:"
echo
echo "    git push origin main"
echo "    git push origin $tag"
