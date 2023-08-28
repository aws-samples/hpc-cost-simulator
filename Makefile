
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
REPODIR := $(mkfile_dir)
REPO := $(shell basename $(REPODIR))
TARBALL := $(REPO).tgz
ZIPFILE := $(REPO).zip

.PHONY: help tests docs fh-docs clean_repo create_tarball

help:
	@echo -e "\nusage: make [ tests | docs | gh-docs | .requirements_installed | clean_repo | create_tarball | zipfile ]"

tests:
	pwd; source ./setup.sh; pytest -v tests

docs:
	mkdocs serve

gh-docs:
	mkdocs gh-deploy

.requirements_installed: requirements.txt
	if [ "${distribution}_${distribution_major_version}" == "RedHat_8" ]; then pip install -r requirements_rhel8.txt; \
	elif [ "${distribution}_${distribution_major_version}" == "RedHat_9" ]; then pip install -r requirements_rhel8.txt; \
	else pip install -r requirements.txt; fi
	touch $@

clean_repo:
	git clean -d -f -x

create_tarball: clean_repo
	cd $(REPODIR)/..; tar --exclude-vcs -czf $(TARBALL) $(REPO)

zipfile: clean_repo
	rm -f $(REPODIR)/../$(ZIPFILE)
	cd $(REPODIR)/..; zip -r $(ZIPFILE) $(REPO) -x '$(REPO)/.git/*'
