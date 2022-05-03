
from os import makedirs, path, system
from os.path import dirname, realpath

def test_security_scan():
    testpath = realpath(__file__)
    testdir = dirname(testpath)
    repopath = realpath(f"{testdir}/..")
    outputdir = path.join(repopath, 'output')
    outputfile = path.join(outputdir, 'security_scan.log')
    makedirs(outputdir, exist_ok=True)
    scriptpath = path.join(testdir, 'security_scan.sh')
    rc = system(f"{scriptpath} &> {outputfile}")
    assert(rc == 0)
