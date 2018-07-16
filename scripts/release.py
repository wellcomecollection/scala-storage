#!/usr/bin/env python

# coding=utf-8
#
# This file is part of Hypothesis, which may be found at
# https://github.com/HypothesisWorks/hypothesis-python
#
# Most of this work is copyright (C) 2013-2017 David R. MacIver
# (david@drmaciver.com), but it contains contributions by others. See
# CONTRIBUTING.rst for a full list of people who may hold copyright, and
# consult the git log if you need to determine who owns an individual
# contribution.
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file, You can
# obtain one at http://mozilla.org/MPL/2.0/.
#
# END HEADER

from __future__ import division, print_function, absolute_import

import os
import sys
import subprocess

import hypothesistooling as tools


if __name__ == '__main__':
    last_release = tools.latest_version()

    print('Latest released version: %s' % last_release)

    HEAD = tools.hash_for_name('HEAD')
    MASTER = tools.hash_for_name('origin/master')

    print('Current head:', HEAD)
    print('Current master:', MASTER)

    on_master = tools.is_ancestor(HEAD, MASTER)
    has_release = tools.has_release()

    if has_release:
        print('Updating changelog and version')
        tools.update_for_pending_release()

    # if not on_master:
    #     print('Not deploying due to not being on master')
    #     sys.exit(0)

    print('Attempting a release.')
    subprocess.check_call(['sbt', 'publish'])

    tools.git('push', 'origin', 'HEAD:master')
