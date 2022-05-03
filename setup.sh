# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

scriptdir=$(dirname $(readlink -f ${BASH_SOURCE[0]}))
repodir=$scriptdir
startdir=$(readlink -f .)

# Deactivate any active virtual envs
deactivate &> /dev/null || true

pushd $repodir &> /dev/null

export PYTHONPATH=$repodir:$PYTHONPATH

# Check for an existing venv first
# If it exists, activate it and make sure it meets requirements
if [ -e .venv/bin/activate ]; then
    echo -e "\nActivating python virtual environment: .venv"
    source .venv/bin/activate
fi

if ! yum list installed make &> /dev/null; then
    echo -e "\nInstalling make"
    if ! sudo yum -y install make; then
        echo -e "\nerror: Could not install make which is required to install packages in the python virtual environment."
        return
    else
        echo "Installed make"
    fi
fi

if ! yum list installed mkdocs &> /dev/null; then
    echo "Installing mkdocs"
    if ! sudo yum -y install mkdocs; then
        echo -e "\nwarning: Could not install mkdocs. Will not be able to display docs in your browsers."
    fi
fi

if ! python3 --version &> /dev/null; then
    echo -e "\nInstalling python3"
    if ! sudo yum -y install python3; then
        echo -e "\nerror: Could not install python3. All of the scripts require python 3.6 or later."
        return
    fi
fi
# Python version >= 3.6 required
# Check python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
python_major_version=$(echo $python_version | cut -d '.' -f 1)
python_minor_version=$(echo $python_version | cut -d '.' -f 2)
if [[ $python_major_version -lt 3 ]] || [[ $python_minor_version -lt 6 ]]; then
    echo -e "\nerror: Script requires python 3.6 or later. You have $python_version."
    # To simplify installation don't require >3.6 which is latest natively supported version on CentOS 7.
    # Left the installation steps for documentation purposes in case needed in the future.
    return
else
    python3_path=$(which python3)
fi
python_version=$(python3 --version 2>&1 | awk '{print $2}')
which python3

# Create python virtual environment
if [ ! -e .venv/bin/activate ]; then
    echo -e "Creating python virtual environment in .venv"
    rm -f .requirements_installed
    python3 -m pip install --upgrade virtualenv
    python3 -m venv .venv
    source .venv/bin/activate
fi

echo -e "\nInstalling python packages in .venv"
python3 -m pip install --upgrade pip
make .requirements_installed

popd &> /dev/null
