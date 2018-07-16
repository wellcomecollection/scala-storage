#!/usr/bin/env python -u
# -*- encoding: utf-8
"""
This script contains all our release tooling for sbt libraries.

Usage:
    sbt_release_tooling.py check_release_file
    sbt_release_tooling.py release
    sbt_release_tooling.py -h | --help

Commands:
    check_release_file      Runs in a pull request to check if the RELEASE.md
                            file is well-formatted.  Exits with 0 if correct,
                            exits with 1 if not.
    release                 Publish a new release of the library.

The canonical version of this script is kept in the platform repo
(https://github.com/wellcometrust/platform), but copied into our other
repos for the sake of easy distribution.

"""

import os
import re
import subprocess
import sys


ROOT = subprocess.check_output([
    'git', 'rev-parse', '--show-toplevel']).decode('ascii').strip()

BUILD_SBT = os.path.join(ROOT, 'build.sbt')


def git(*args):
    """
    Run a Git command and check it completes successfully.
    """
    subprocess.check_call(('git',) + args)


def tags():
    """
    Returns a list of all tags in the repo.
    """
    git('fetch', '--tags')
    result = subprocess.check_output(['git', 'tag']).decode('ascii').strip()
    all_tags = result.split('\n')

    assert len(set(all_tags)) == len(all_tags)

    return set(all_tags)


def latest_version():
    """
    Returns the latest version, as specified by the Git tags.
    """
    versions = []

    for t in tags():
        assert t == t.strip()
        parts = t.split('.')
        assert len(parts) == 3, t
        parts[0] = parts[0].lstrip('v')
        v = tuple(map(int, parts))

        versions.append((v, t))

    _, latest = max(versions)

    assert latest in tags()
    return latest


def modified_files():
    """
    Returns a list of all files which have been modified between now
    and the latest release.
    """
    files = set()
    for command in [
        ['git', 'diff', '--name-only', '--diff-filter=d',
            latest_version(), 'HEAD'],
        ['git', 'diff', '--name-only']
    ]:
        diff_output = subprocess.check_output(command).decode('ascii')
        for l in diff_output.split('\n'):
            filepath = l.strip()
            if filepath:
                assert os.path.exists(filepath)
                files.add(filepath)
    return files


def has_source_changes():
    """
    Returns True if there are source changes since the previous release,
    False if not.
    """
    changed_files = [
        f for f in modified_files() if f.strip().endswith(('.sbt', '.scala'))
    ]
    return len(changed_files) != 0


RELEASE_FILE = os.path.join(ROOT, 'RELEASE.md')


def has_release():
    """
    Returns True if there is a release file, False if not.
    """
    return os.path.exists(RELEASE_FILE)


RELEASE_TYPE = re.compile(r"^RELEASE_TYPE: +(major|minor|patch)")

MAJOR = 'major'
MINOR = 'minor'
PATCH = 'patch'

VALID_RELEASE_TYPES = (MAJOR, MINOR, PATCH)


def parse_release_file():
    """
    Parses the release file, returning a tuple (release_type, release_contents)
    """
    with open(RELEASE_FILE) as i:
        release_contents = i.read()

    release_lines = release_contents.split('\n')

    m = RELEASE_TYPE.match(release_lines[0])
    if m is not None:
        release_type = m.group(1)
        if release_type not in VALID_RELEASE_TYPES:
            print('Unrecognised release type %r' % (release_type,))
            sys.exit(1)
        del release_lines[0]
        release_contents = '\n'.join(release_lines).strip()
    else:
        print(
            'RELEASE.md does not start by specifying release type. The first '
            'line of the file should be RELEASE_TYPE: followed by one of '
            'major, minor, or patch, to specify the type of release that '
            'this is (i.e. which version number to increment). Instead the '
            'first line was %r' % (release_lines[0],)
        )
        sys.exit(1)

    return release_type, release_contents


def check_release_file():
    if has_source_changes():
        if not has_release():
            print(
                'There are source changes but no RELEASE.md. Please create '
                'one to describe your changes.'
            )
            sys.exit(1)
        parse_release_file()


if __name__ == '__main__':

    # Rudimentary command-line argument parsing.
    #
    # It would be nice to replace this with something more robust using
    # argparse or docopt, but installing extra packages in Travis when
    # releasing a Scala library is more hassle than it's worth.
    #
    if (
        len(sys.argv) != 2 or
        sys.argv[1] in ('-h', '--help') or
        sys.argv[1] not in ('check_release_file', 'release')
    ):
        print(__doc__.strip())
        sys.exit(1)

    if sys.argv[1] == 'check_release_file':
        check_release_file()
