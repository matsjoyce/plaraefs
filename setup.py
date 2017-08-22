#!/usr/bin/env python3

import subprocess
import shutil
from setuptools import setup
from plaraefs import fusefilesystem
from setuptools.command import build_ext, install


class BuildExt(build_ext.build_ext):
    def run(self):
        super().run()
        print("Compiling exe")
        subprocess.check_call(["g++", "-o", "build/plaraefs", "plaraefs/plaraefs.cpp", "--std=c++11", "-O2", "-lcap", "-lcap-ng"])


class Install(install.install):
    def run(self):
        super().run()
        print("Copying exe")
        shutil.copy("build/plaraefs", "/usr/bin/plaraefs")
        print("Setting caps")
        subprocess.call(["setcap", "cap_sys_ptrace+epi", "/usr/bin/plaraefs"])


setup(name="plaraefs",
      version="0.1",
      author="Matthew Joyce",
      author_email="matsjoyce@gmail.com",
      packages=["plaraefs", "plaraefs.accesscontroller"],
      package_data={"plaraefs": ["include/*"]},
      entry_points={"console_scripts": ["plaraefs = plaraefs:main"]},
      ext_modules=[
          fusefilesystem.ffi.verifier.get_extension(),
      ],
      install_requires=[
          "cffi",
      ],
      cmdclass={
          "install": Install,
          "build_ext": BuildExt
      })
