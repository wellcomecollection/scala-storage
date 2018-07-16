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


ROOT = subprocess.check_output([
    'git', 'rev-parse', '--show-toplevel']).decode('ascii').strip()

BUILD_SBT = os.path.join(ROOT, 'build.sbt')


def current_version():
    """Returns the current version, as set in build.sbt."""
    # We're looking for a line of the form
    #
    #       version := "x.y.z"
    #
    with open(BUILD_SBT) as infile:
        for line in infile:
            m = re.match(r'^version := "(?P<version>\d+\.\d+\.\d+)"\n$', line)
            if m is not None:
                return m.group('version')

    raise ValueError('Unable to find a version in build.sbt?')
