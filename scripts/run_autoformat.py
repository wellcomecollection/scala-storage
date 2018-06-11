# -*- encoding: utf-8
"""
This script does autoformatting in Travis CI on pull requests.
"""

import os
import subprocess
import sys

from config import REPO_NAME
from hypothesistooling import ROOT, branch_name, git


if __name__ == '__main__':
    subprocess.check_call([
        'docker', 'run', '--rm',
        '--volume', '%s/.sbt:/root/.sbt' % os.environ['HOME'],
        '--volume', '%s/.ivy2:/root/.ivy2' % os.environ['HOME'],
        '--volume', '%s:/repo' % ROOT,
        'wellcome/scalafmt'
    ])

    # If there are any changes, push to GitHub immediately and fail the
    # build.  This will abort the remaining jobs, and trigger a new build
    # with the reformatted code.
    if subprocess.call(['git', 'diff', '--exit-code']):
        print('*** There were changes from formatting, creating a commit')

        git('config', 'user.name', 'Travis CI on behalf of Wellcome')
        git('config', 'user.email', 'wellcomedigitalplatform@wellcome.ac.uk')
        git('config', 'core.sshCommand', 'ssh -i secrets/id_rsa')

        git(
            'remote', 'add', 'ssh-origin',
            'git@github.com:wellcometrust/%s.git' % REPO_NAME
        )

        # We checkout the branch before we add the commit, so we don't
        # include the merge commit that Travis makes.
        git('fetch', 'ssh-origin')
        git('checkout', branch_name())

        git('add', '--verbose', '--all')
        git('commit', '-m', 'Apply auto-formatting rules')
        git('push', 'ssh-origin', 'HEAD:%s' % branch_name())

        # We exit here to fail the build, so Travis will skip to the next
        # build, which includes the autoformat commit.
        sys.exit(1)
    else:
        print('*** There were no changes from auto-formatting')
