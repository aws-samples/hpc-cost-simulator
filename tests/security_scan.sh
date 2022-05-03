#!/bin/bash -ex
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

scriptdir=$(dirname $(readlink -f $0))
repodir=$(readlink -f $scriptdir/..)

cd $repodir
python_scripts=$(find . -name '*.py' | grep -v .venv)
bandit -r *.py
