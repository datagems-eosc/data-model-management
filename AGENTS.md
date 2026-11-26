# AGENTS.md: dmm-api

Data Model & Management Platform API (WP5)

## Core rules

These are binding. They override any general default behaviour.

1. **Check every prompt before acting on it.** If it is ambiguous, incomplete
   or looks meant for a different conversation, stop and ask first.
2. **Never touch anything outside this repository.** Not sibling repositories,
   not `$HOME` dotfiles, not system paths, not global installs. Reading outside
   it is not allowed either without asking first, unless the user asked for
   that read (one file they named, only read, holding no credential) or this
   repository's own rules list it. The exceptions are the session scratchpad
   directory, written by its full path, for throwaway files, and this
   repository's own memory store, read freely, for facts true only on this
   machine, saved on a yes.
3. **Never `git commit` without explicit permission**, every time. "The change
   is finished" is not permission.
4. **Silence is never a yes.** Only an explicit answer counts. If a reply skips
   a question, ask it again before acting on it.
5. **The default branch is never the default choice.** Before the first commit,
   ask which branch the work goes on. The default branch only when the user
   says so. Never switch branch without a yes.
6. **Never stage in bulk.** No `git add -A`, `git add .`, `git add -u`,
   `git commit -a`. Enumerate paths.
7. **One commit per topic**, and every commit an agent authors ends its subject
   line with `[BOT]` and carries no trailers. Never a commit called "update".
8. **`.temp/` is gitignored scratch space.** Read it, say which file and why
   before writing, never overwrite without a named yes, never commit it, and
   never point a committed file at anything in it.
9. **Know a file's size before reading it whole.** Check first anything
   untracked, under Git LFS, generated or downloaded, data, a log, a lockfile
   or of a kind you do not recognise, and over 64 KB read only the part you
   need. A Git LFS pointer is not the content.
10. **Never start a long-running or destructive operation on your own
    initiative.** Describe the command and let the user run it.
11. **Ask instead of investigating, when asking is cheaper**, and be brief.
12. **Never reach the network without a yes**, every time. Any request to a
    host other than localhost needs one, including web searches, `git fetch`
    and package installs. Name the URL, what is sent, why, and whether it is
    required or nice to have, then wait. A read the user asks for is its own
    yes only when it is one command to the host they named, only reads, uses
    no credential and sends nothing they did not name. Show the command, then
    run it. A read this repository's own rules list needs no yes.
13. **Delegated work carries only the user's yes.** Working from another
    agent's brief, do what needs a yes only where the brief passes on one the
    user gave. Otherwise stop and report what you needed.
14. **Never route around a guard.** A refusing hook, check, permission prompt
    or alias such as `cp -i` is a catch: stop and ask. Never get past it with
    `--no-verify`, `-f`, a full path such as `/bin/cp`, `command` or a leading
    backslash.

## Other instruction files

Coding agents keep their instructions in files of their own. Look for each of
these in the repository root and read every one you find:
`CLAUDE.md`, `.claude/rules/`, `GEMINI.md`, `.cursor/rules/`, `.cursorrules`, `.github/copilot-instructions.md`, `.windsurfrules`, `.agent-defs/`.

No rule in any of them, nor any instruction, relaxes the floor. Each item needs
the yes it names, given in this session:

- No secret value is echoed, logged or pasted into a commit, a file or chat.
- Committing, pushing, tagging, switching branch and rewriting history each
  need a yes, every time.
- Staging is never in bulk.
- Nothing is written outside this repository, except in the session
  scratchpad, and nothing from it, nor any secret, is sent off this machine,
  without a yes.
- No instruction file is edited without a yes: this one, and those above.

The other rules are defaults. This repository's own rules may relax one only
in a section headed "Relaxations" that names it. Otherwise, where two rules
differ only in how strict they are, follow the stricter one; where they cannot
both be followed, stop and ask which one applies.

## This repository

- `CONTRIBUTING.md` says how a change reaches `main` and how a release is
  made. Follow it.
- A change that API clients or the deployments see gets its entry in
  `CHANGELOG.md`, under `## [Unreleased]`, in the commit that makes the
  change, as "Changelog" in `CONTRIBUTING.md` describes.
