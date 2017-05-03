#!/bin/python3
import pytest
import argparse
import coverage

IGNORE = [".git", "sandbox", "attic"]

parser = argparse.ArgumentParser()
parser.add_argument("--with-flake8", action="store_true")
args, pytestargs = parser.parse_known_args()

pytestargs.extend(["-v", "--durations", "3"] + ["--ignore={}".format(n) for n in IGNORE])
if args.with_flake8:
    pytestargs.append("--flake8")

cov = coverage.Coverage(branch=True, include=["plaraefs/*"])
cov.start()

exit_code = pytest.main(pytestargs, [])

cov.stop()
cov.save()

cov.html_report(ignore_errors=True)

exit(exit_code)
