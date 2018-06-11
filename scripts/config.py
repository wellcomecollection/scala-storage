# -*- encoding: utf-8

import os


def has_source_changes(version=None):
    from hypothesistooling import latest_version, modified_files

    if version is None:
        version = latest_version()

    changed_files = [
        f for f in modified_files() if f.strip().endswith(('.sbt', '.scala'))
    ]
    return len(changed_files) != 0
