#!/usr/bin/env python
# encoding: utf-8

"""pywebhdfs: a command line interface for WEb HDFS.

Usage:
  pywebhdfs [-c CLUSTER] [--conf=CONFIGURATION] [-v...]
  pywebhdfs (-V | -h)

Options:
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  -V --version                  Show version and exit.
  -h --help                     Show help and exit.
  -c CLUSTER --cluster=CLUSTER  Cluster configuration to connect to.
  --conf=CONFIGURATION          pywhdfs configuration file to use. Defauls to ~/.webhdfs.cfg and could
                                be set using the environement variable WEBHDFS_CONFIG.
Examples:
  pywebhdfs -a prod

pywebhdfs exits with return status 1 if an error occurred and 0 otherwise.

"""

from . import __version__
from .config import WebHDFSConfig
from .utils import hglob
from .utils.utils import *
from docopt import docopt
from threading import Lock
import logging as lg
import os.path as osp
import requests as rq
import os
import sys
import glob

def configure_client(args):
  """Instantiate configuration from arguments dictionary.
  :param args: Arguments returned by `docopt`.
  :param config: CLI configuration, used for testing.

  If the `--log` argument is set, this method will print active file handler
  paths and exit the process.
  """
  
  # capture warnings issued by the warnings module  
  try:
    # This is not available in python 2.6
    lg.captureWarnings(True)
  except:
    # disable annoying url3lib warnings
    rq.packages.urllib3.disable_warnings()
    pass

  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)
  lg.getLogger('requests_kerberos.kerberos_').setLevel(lg.CRITICAL)
  lg.getLogger('requests').setLevel(lg.ERROR)
  # logger.addFilter(AnnoyingErrorsFilter())

  levels = {0: lg.CRITICAL, 1: lg.ERROR, 2: lg.WARNING, 3: lg.INFO}

  # Configure stream logging if applicable
  stream_handler = lg.StreamHandler()
  # This defaults to zero
  stream_log_level=levels.get(args['--verbose'], lg.DEBUG)
  stream_handler.setLevel(stream_log_level)

  fmt = '%(levelname)s\t%(message)s'
  stream_handler.setFormatter(lg.Formatter(fmt))
  logger.addHandler(stream_handler)

  config = WebHDFSConfig(args['--conf'])

  # configure file logging if applicable
  handler = config.get_log_handler()
  logger.addHandler(handler)

  return config.get_client(args['--cluster'])

def main(argv=None):
  """Entry point.
  :param argv: Arguments list.
  :param client: For testing.
  """

  args = docopt(__doc__, argv=argv, version=__version__)
  client = configure_client(args)

  banner = (
      '\n'
      '---------------------------------------------------\n'
      '------------------- PYWEBHDFS %s ------------------\n'
      '-- Welcome to WEB HDFS interactive python shell. --\n'
      '-- The WEB HDFS client is available as `CLIENT`. --\n'
      '--------------------- Enjoy ! ---------------------\n'
      '---------------------------------------------------\n'
      '\n'
  ) % __version__

  namespace = {'CLIENT': client}
  
  try:
    from IPython import embed
  except ImportError:
    from code import interact
    interact(banner=banner, local=namespace)
  else:
    embed(banner1=banner, user_ns=namespace)

if __name__ == '__main__':
  main()
