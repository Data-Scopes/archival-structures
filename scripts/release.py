#!/usr/bin/env python
"""Version bumping and releases for archival-structures.

The version lives in one place, ``[project] version`` in ``pyproject.toml``, and a release is
an annotated git tag ``v<version>`` on a commit whose ``pyproject.toml`` carries that same
version. This script keeps the two in step: it refuses to tag when they would disagree (as
happened with ``v0.4.0``, tagged while ``pyproject.toml`` still said ``0.3.0``).

Commands::

    python scripts/release.py version            # current version, latest tag, consistency
    python scripts/release.py check              # audit every v* tag against its pyproject.toml
    python scripts/release.py bump patch         # or minor / major / an explicit X.Y.Z
    python scripts/release.py release            # test, commit the bump, create the tag
    python scripts/release.py release --push     # ... and push the branch and the tag
    python scripts/release.py release --push --github   # ... and create a GitHub release

``bump`` only edits ``pyproject.toml``. ``release`` commits it (message ``Bump version to
X.Y.Z``), creates the tag, and optionally pushes and publishes; add ``--dry-run`` to print the
steps without running them. Only the Python standard library and ``git`` are needed; ``gh`` is
needed only for ``--github``.
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / 'pyproject.toml'
TAG_PREFIX = 'v'
VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
DEFAULT_BRANCH = 'main'
DEFAULT_TEST_COMMAND = '.venv/bin/python -m pytest -q -p no:cacheprovider'
#: published tags whose pyproject.toml disagrees and that cannot be fixed after the fact;
#: `check` reports them without failing
KNOWN_MISMATCHES = {
    'v0.4.0': 'tagged on 8b6079e before the version bump; v0.4.1 has the same code',
}


class ReleaseError(Exception):
    pass


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

def parse_version(text: str) -> tuple[int, int, int]:
    match = VERSION_PATTERN.match(text)
    if not match:
        raise ReleaseError(f'not a MAJOR.MINOR.PATCH version: {text!r}')
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def bumped(version: str, rule: str) -> str:
    """The version after applying *rule* (``major``/``minor``/``patch`` or an explicit X.Y.Z)."""
    if rule not in ('major', 'minor', 'patch'):
        parse_version(rule)
        return rule
    major, minor, patch = parse_version(version)
    if rule == 'major':
        return f'{major + 1}.0.0'
    if rule == 'minor':
        return f'{major}.{minor + 1}.0'
    return f'{major}.{minor}.{patch + 1}'


def pyproject_version(text: str) -> str:
    return tomllib.loads(text)['project']['version']


def set_pyproject_version(text: str, new_version: str) -> str:
    """Replace the ``[project]`` version line only, leaving all other formatting untouched."""
    section = re.search(r'(?ms)^\[project\]\s*$(.*?)(?=^\[|\Z)', text)
    if not section:
        raise ReleaseError('no [project] table in pyproject.toml')
    body, count = re.subn(r'(?m)^(version\s*=\s*)"[^"]*"', rf'\g<1>"{new_version}"',
                          section.group(1), count=1)
    if count != 1:
        raise ReleaseError('no version line in the [project] table of pyproject.toml')
    updated = text[:section.start(1)] + body + text[section.end(1):]
    assert pyproject_version(updated) == new_version
    return updated


# ---------------------------------------------------------------------------
# Git
# ---------------------------------------------------------------------------

def git(*args: str) -> str:
    result = subprocess.run(['git', *args], cwd=REPO_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise ReleaseError(f'git {" ".join(args)} failed: {result.stderr.strip()}')
    return result.stdout.strip()


def version_tags() -> list[str]:
    """``v*`` tags with a valid version, highest version first."""
    tags = [t for t in git('tag', '--list', f'{TAG_PREFIX}*').splitlines()
            if VERSION_PATTERN.match(t[len(TAG_PREFIX):])]
    return sorted(tags, key=lambda t: parse_version(t[len(TAG_PREFIX):]), reverse=True)


def tagged_pyproject_version(tag: str) -> str | None:
    try:
        return pyproject_version(git('show', f'{tag}:pyproject.toml'))
    except (ReleaseError, KeyError, tomllib.TOMLDecodeError):
        return None


def run(command: list[str], dry_run: bool) -> None:
    print('  $', shlex.join(command))
    if not dry_run:
        subprocess.run(command, cwd=REPO_ROOT, check=True)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_version(_args: argparse.Namespace) -> int:
    current = pyproject_version(PYPROJECT.read_text(encoding='utf-8'))
    tags = version_tags()
    latest = tags[0] if tags else None
    head_tags = git('tag', '--points-at', 'HEAD').split()
    print(f'pyproject.toml version: {current}')
    print(f'latest tag:             {latest or "(none)"}')
    print(f'tags on HEAD:           {" ".join(head_tags) or "(none)"}')
    if latest and parse_version(current) <= parse_version(latest[len(TAG_PREFIX):]):
        print(f'next release needs a bump: {current} is not above {latest}')
    return 0


def cmd_check(_args: argparse.Namespace) -> int:
    problems = 0
    for tag in version_tags():
        found = tagged_pyproject_version(tag)
        expected = tag[len(TAG_PREFIX):]
        if found == expected:
            status = 'ok'
        elif tag in KNOWN_MISMATCHES:
            status = f'known mismatch (pyproject.toml says {found}): {KNOWN_MISMATCHES[tag]}'
        else:
            status = f'MISMATCH: pyproject.toml says {found}'
            problems += 1
        print(f'{tag:10s} {status}')
    return 1 if problems else 0


def cmd_bump(args: argparse.Namespace) -> int:
    text = PYPROJECT.read_text(encoding='utf-8')
    current = pyproject_version(text)
    tags = version_tags()
    # bump from whichever is higher, so a tag made without a bump is never reused
    base = current
    if tags and parse_version(tags[0][len(TAG_PREFIX):]) > parse_version(current):
        base = tags[0][len(TAG_PREFIX):]
        print(f'note: latest tag {tags[0]} is ahead of pyproject.toml ({current}); bumping from {base}')
    new_version = bumped(base, args.rule)
    if tags and parse_version(new_version) <= parse_version(tags[0][len(TAG_PREFIX):]):
        raise ReleaseError(f'{new_version} is not above the latest tag {tags[0]}')
    PYPROJECT.write_text(set_pyproject_version(text, new_version), encoding='utf-8')
    print(f'pyproject.toml: {current} -> {new_version}')
    print('next: python scripts/release.py release  (add --push / --github as needed)')
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    version = pyproject_version(PYPROJECT.read_text(encoding='utf-8'))
    tag = f'{TAG_PREFIX}{version}'
    tags = version_tags()

    # preconditions, all checked before anything is changed
    branch = git('branch', '--show-current')
    if branch != args.branch:
        raise ReleaseError(f'on branch {branch!r}, releases are made from {args.branch!r}')
    if tag in tags:
        raise ReleaseError(f'tag {tag} already exists; bump the version first')
    if tags and parse_version(version) <= parse_version(tags[0][len(TAG_PREFIX):]):
        raise ReleaseError(f'{version} is not above the latest tag {tags[0]}; bump first')
    dirty = [line for line in git('status', '--porcelain', '--untracked-files=no').splitlines()
             if line[3:] != 'pyproject.toml']
    if dirty:
        raise ReleaseError('uncommitted changes to tracked files (commit or stash them first):\n'
                           + '\n'.join(dirty))
    if args.github and subprocess.run(['gh', 'auth', 'status'], capture_output=True).returncode:
        raise ReleaseError('--github needs the GitHub CLI logged in: run `gh auth login`')

    print(f'releasing {tag}' + (' (dry run)' if args.dry_run else ''))
    if not args.skip_tests:
        run(shlex.split(args.test_command), args.dry_run)
    if git('status', '--porcelain', '--untracked-files=no', '--', 'pyproject.toml'):
        run(['git', 'add', 'pyproject.toml'], args.dry_run)
        run(['git', 'commit', '-m', f'Bump version to {version}'], args.dry_run)
    run(['git', 'tag', '-a', tag, '-m', f'Release {version}'], args.dry_run)
    if args.push:
        run(['git', 'push', 'origin', args.branch], args.dry_run)
        run(['git', 'push', 'origin', tag], args.dry_run)
    if args.github:
        previous = tags[0] if tags else None
        log_range = f'{previous}..{tag}' if previous else tag
        notes = (git('log', '--format=- %s', log_range) if not args.dry_run else
                 f'(commit subjects from git log {log_range})')
        run(['gh', 'release', 'create', tag, '--title', tag, '--notes', notes], args.dry_run)
    if not args.push:
        print(f'tag created locally; publish with: git push origin {args.branch} {tag}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('version', help='show version, latest tag and consistency')
    commands.add_parser('check', help='verify that every v* tag matches its pyproject.toml')
    bump = commands.add_parser('bump', help='set a new version in pyproject.toml')
    bump.add_argument('rule', help='major, minor, patch, or an explicit X.Y.Z')
    release = commands.add_parser('release', help='test, commit the bump and tag')
    release.add_argument('--push', action='store_true', help='push the branch and the tag')
    release.add_argument('--github', action='store_true', help='create a GitHub release (gh)')
    release.add_argument('--dry-run', action='store_true', help='print the steps only')
    release.add_argument('--skip-tests', action='store_true')
    release.add_argument('--test-command', default=DEFAULT_TEST_COMMAND)
    release.add_argument('--branch', default=DEFAULT_BRANCH)
    args = parser.parse_args()
    handler = {'version': cmd_version, 'check': cmd_check, 'bump': cmd_bump,
               'release': cmd_release}[args.command]
    try:
        return handler(args)
    except ReleaseError as error:
        print(f'error: {error}', file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        print(f'error: step failed ({shlex.join(error.cmd)}); nothing after it was run',
              file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
