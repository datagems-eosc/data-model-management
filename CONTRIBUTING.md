# Contributing

How a change reaches `main`, and how a release reaches the clusters. Set up
the environment first, as the README describes under "Getting started".

## Making a change

1. Branch from an up-to-date `main`.
2. Before pushing, run the checks and the tests:

   ```
   make check
   make test
   ```

3. Open a pull request against `main`. Every change reaches `main` through a
   pull request.

In the pull request description, say:

- which environment variables are added, renamed or removed, with their
  defaults, and whether the service starts without them. Update the tables in
  the README as well. The dev and prod deployments set every variable
  explicitly, so they have to change before they can run the release that
  needs it.
- what changes for clients of the API, if anything.

Change dependencies through uv (`uv add`, `uv remove`, `uv lock`), so that
`pyproject.toml` and `uv.lock` stay in step. Never edit `uv.lock` by hand.

## Formatting-only commits and git blame

Some commits only reformat files, such as `c9c143c` ("chore: apply the
pre-commit hooks to every file"), which first ran the pre-commit hooks over
the whole repository. `git blame` would show them as the last change to
every line they touched, hiding who really wrote it. `.git-blame-ignore-revs`
lists those commits, so that blame skips them and shows the change before.

GitHub's blame view reads the file on its own. For `git blame` in your clone,
run this once:

```
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

To add a formatting-only commit, put its full hash in the file, under a
comment line with its subject, in a commit of its own after it. Merge that
pull request with a merge commit, not squash or rebase: both create new
commits, and the listed hash would not be on `main`.

## Releasing

A release is made on `main`, after the pull requests it releases are merged.
Versions follow semantic versioning, `MAJOR.MINOR.PATCH`.

1. Make the release commit and its tag, with the new version number:

   ```
   make release VERSION=<X.Y.Z>
   ```

   It checks that you are on `main`, with no uncommitted changes and in step
   with `origin/main`. Then it sets the version in `pyproject.toml` and
   `uv.lock`, commits only that, and tags the commit `v<X.Y.Z>`.

2. Push the commit and the tag, as the command prints:

   ```
   git push origin main
   git push origin v<X.Y.Z>
   ```

3. The tag starts the image workflow. The image is published once the run for
   the tag finishes green:

   ```
   https://github.com/datagems-eosc/data-model-management/actions/workflows/docker-image.yml
   ```

The workflow builds only from a tag that is `v` followed by the version in
`pyproject.toml`: it refuses a tag on a commit with a different version, and a
tag without the `v` starts no build at all. `make release` gets both right.
Never move or delete a pushed tag. If a release is wrong, fix it in the next
one.

## Deploying

A release reaches a cluster when its deployment repository pins the new image
tag: first `data-model-management-deployment-dev`, then, once dev runs it,
`data-model-management-deployment-prod`. Tell whoever maintains them about any
environment variable the release adds, renames or removes.
