# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

scriptdir=$(dirname $(readlink -f ${BASH_SOURCE[0]}))
repodir=$scriptdir
startdir=$(readlink -f .)

# Detect the OS distribution and version
if [ -e /etc/os-release ]; then
    source /etc/os-release
    if [ $ID == "amzn" ]; then
        export distribution=Amazon
    elif [ $ID == "centos" ]; then
        export distribution=CentOS
    elif [ $ID == "rhel" ]; then
        export distribution=RedHat
    else
        echo -e "\nerror: Unsupported OS distribution $ID"
        return 1
    fi
    export distribution_version=$VERSION_ID
    export distribution_major_version=$(echo $distribution_version | cut -d '.' -f 1)
else
    echo -e "\nerror: Could not detect the OS distribution. /etc/os-release doesn't exist."
    return 1
fi
echo -e "Setting up for $distribution $distribution_major_version\n"

# Deactivate any active virtual envs
timeout 1s deactivate &> /dev/null || true

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
        echo -e "\nInstalled make\n"
    fi
fi

if ! yum list installed mkdocs &> /dev/null; then
    echo -e "\nInstalling mkdocs\n"
    if ! sudo yum -y install mkdocs; then
        echo -e "\nwarning: Could not install mkdocs. Will not be able to display docs in your browsers.\n"
    else
        echo -e "\nInstalled mkdocs\n"
    fi
fi

if ! python3 --version &> /dev/null; then
    echo -e "\nInstalling python3\n"
    if ! sudo yum -y install python3; then
        echo -e "\nerror: Could not install python3. All of the scripts require python 3.6 or later."
        return
    else
        echo -e "\nInstalled python3\n"
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
fi
echo -e "\nUsing python $python_version\n"

# Create python virtual environment
if [ ! -e .venv/bin/activate ]; then
    echo -e "\nCreating python virtual environment in .venv\n"
    rm -f .requirements_installed
    python3 -m pip install --upgrade virtualenv
    python3 -m venv .venv
    source .venv/bin/activate
fi

echo -e "\nTrying to upgrade venv pip version.\n"
python3 -m pip install --upgrade pip

echo -e "\nInstalling python packages in .venv\n"
if ! make .requirements_installed; then
    echo -e "\nFailed to install some Python packages\n"
fi

# Make sure you have the latest AWSCLI
echo -e "\nUpgrading the boto3 version used in the venv\n"
pip install --upgrade boto3

popd &> /dev/null
