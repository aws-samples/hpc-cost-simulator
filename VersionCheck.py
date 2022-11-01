#!/usr/bin/env python3
'''
Check that the git repository has the latest version checked out.
'''

import argparse
from git import InvalidGitRepositoryError, NoSuchPathError, Repo
import logging
from os.path import dirname, realpath
from sys import exit

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False

class VersionCheck:
    '''
    '''

    def __init__(self):
        '''
        Constructor

        Args:
        Raises:
        Returns:
            bool: True is not a git repository or current branch has latest version from origin.
        '''
        pass

    def check_git_version(self) -> bool:
        '''
        Raises:
        Returns:
            bool: True is not a git repository or current branch has latest version from origin.
        '''
        repo_dir = dirname(realpath(__file__))
        logger.debug(f"Git repo dir: {repo_dir}")
        try:
            repo = Repo(repo_dir)
        except InvalidGitRepositoryError:
            logger.debug(f"{repo_dir} is not a valid git repo")
            return True
        if repo.bare:
            logger.debug(f"{repo_dir} is bare")
            return True

        # If dirty, don't check origin.
        if repo.is_dirty():
            answer = ''
            while answer not in ['y', 'n']:
                logger.info(f"The repo is dirty. Do you want to continue? (y/n)")
                answer = input().lower()
            return answer == 'y'

        # Check to see if current branch is up-to-date with origin.
        for head in repo.heads:
            logger.debug(f"head: {head.name:80} {head.commit}")
        branch = repo.active_branch
        logger.debug(f"Current branch: {branch.name} ({branch.commit})")
        local_commit = branch.commit
        origin = repo.remote('origin')
        origin_info = origin.fetch(f"{branch}")[0]
        origin_commit = origin_info.commit
        logger.debug(f"origin/{branch} ({origin_commit}")
        if local_commit == origin_commit:
            return True
        # Check if is behind the remote and needs a pull
        commits_behind = repo.iter_commits(f"{branch}..origin/{branch}")
        num_commits_behind = sum(1 for c in commits_behind)
        if num_commits_behind:
            answer = ''
            while answer not in ['y', 'n']:
                logger.info(f"{branch} is {num_commits_behind} commits behind origin/{branch}. Do you want to \"pull --rebase\"? (y/n)")
                answer = input().lower()
            if answer == 'n':
                return True
            origin.pull(rebase=True)

        commits_ahead = repo.iter_commits(f"origin/{branch}..{branch}")
        num_commits_ahead = sum(1 for c in commits_ahead)
        logger.debug(f"{branch} is {num_commits_ahead} commits ahead of origin/{branch}.")
        return True

def main():
    parser = argparse.ArgumentParser(description="Check git version.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    checker = VersionCheck()
    return checker.check_git_version()

if __name__ == '__main__':
    if not main():
        exit(1)
