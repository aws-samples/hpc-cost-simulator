
from os import makedirs, path, system
from os.path import dirname, realpath
import pytest
from test_scaling import order as last_order
import unittest

order = last_order // 100 * 100 + 100
assert order == 900

class TestSecurity(unittest.TestCase):
    global order

    def __init__(self, name):
        super().__init__(name)

    def cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    order += 1
    @pytest.mark.order(order)
    def test_security_scan(self):
        self.cleanup_output_files()

        testpath = realpath(__file__)
        testdir = dirname(testpath)
        repopath = realpath(f"{testdir}/..")
        outputdir = path.join(repopath, 'output')
        outputfile = path.join(outputdir, 'security_scan.log')
        makedirs(outputdir, exist_ok=True)
        scriptpath = path.join(testdir, 'security_scan.sh')
        rc = system(f"{scriptpath} &> {outputfile}")
        assert(rc == 0)
