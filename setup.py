#!/usr/bin/env python
from setuptools import setup
import re
import os


_packages = {
    'aiodocker_registry': 'aiodocker_registry',
}


def my_test_suite():
    import asynctest
    test_loader = asynctest.TestLoader()
    test_suite = test_loader.discover('tests')
    return test_suite


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'rate_limiter', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in '
                               'rate_limiter/__init__.py')


setup(
    name="aiodocker-registry",
    version=read_version(),
    description='aiodocker registry client',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    author='Alexander Mohr',
    author_email='thehesiod@gmail.com',
    url='https://github.com/thehesiod/aiodocker_registry',
    package_dir=_packages,
    packages=list(_packages.keys()),
    install_requires=[
        'aiohttp>=3.3.1',
        'asyncpool'
    ],
    test_suite='setup.my_test_suite',
)
