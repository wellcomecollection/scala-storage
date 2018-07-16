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
import subprocess


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
    result = subprocess.check_output(['git', 'tag']).decode('ascii')
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
