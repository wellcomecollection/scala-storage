# -*- encoding: utf-8

from hypothesistooling import modified_files


REPO_NAME = 'scala-storage'

TRAVIS_KEY = os.environ['encrypted_12c8071d2874_key']
TRAVIS_IV = os.environ['encrypted_12c8071d2874_iv']


def has_source_changes(version=None):
    if version is None:
        version = latest_version()

    changed_files = [
        f for f in modified_files() if f.strip().endswith('.sbt', '.scala')
    ]
    return len(changed_files) != 0
