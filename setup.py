#!/usr/bin/env python3

from setuptools import setup

setup(name="plaraefs",
      version="0.1",
      author="Matthew Joyce",
      author_email="matsjoyce@gmail.com",
      packages=["plaraefs"],
      entry_points={"console_scripts": ["plaraefs = plaraefs:main"]})
