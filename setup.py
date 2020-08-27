#!/usr/bin/env python

"""pywhdfs: python Web HDFS Client."""

import os
import sys
import re
from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath('src'))

def _get_version():
  """Extract version from package."""
  with open('pywhdfs/__init__.py') as reader:
    match = re.search(
      r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
      reader.read(),
      re.MULTILINE
    )
    if match:
      return match.group(1)
    else:
      raise RuntimeError('Unable to extract version.')

def _get_long_description():
  """Get README contents."""
  with open('README') as reader:
    return reader.read()

setup(
  name='pywhdfs',
  version=_get_version(),
  description=__doc__,
  long_description=_get_long_description(),
  author='Yassine Azzouz',
  author_email='yassine.azzouz@agmail.com',
  url='https://bitbucket.org/suty/pyhdfs',
  license='MIT',
  packages=find_packages(),
  package_data={'': ['*.conf','*.json']},
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
  ],
  install_requires=[
    'docopt',
    'requests>=2.7.0',
    'jsonschema>=2.0',
    'requests-kerberos>=0.7.0',
    'pykerberos',
    'python-krbV'
  ],
  entry_points={'console_scripts': 
     [ 'pywhdfs = pywhdfs.cmdtool:main' ]
  },
  long_description_content_type='text/markdown'
)
