#!/usr/bin/env python
# encoding: utf-8

import ast
from .client import *
from pkg_resources import resource_string
from functools import wraps
from imp import load_source
from logging.handlers import TimedRotatingFileHandler
from tempfile import gettempdir
from copy import deepcopy
import logging as lg
import os
import os.path as osp
import jsonschema as js
import sys
import json

_logger = lg.getLogger(__name__)

class WebHDFSConfig(object):

  default_path = osp.expanduser('~/.webhdfs.cfg')

  def __init__(self, path=None):
      self.path = path or os.getenv('WEBHDFS_CONFIG', self.default_path)
      if osp.exists(self.path):
        try:
          self.config = json.loads(open(self.path).read())
          self.schema = json.loads(resource_string(__name__, 'resources/config_schema.json'))
          #self.schema = open("resources/schema.config").read()
          try:
            js.validate(self.config, self.schema)
          except js.ValidationError as e:
            print e.message
          except js.SchemaError as e:
            print e

        except ParsingError:
          raise HdfsError('Invalid configuration file %r.', self.path)

        _logger.info('Instantiated configuration from %r.', self.path)
      else:
        raise HdfsError('Invalid configuration file %r.', self.path)


  def get_log_handler(self):
      """Configure and return file logging handler."""
      path = osp.join(gettempdir(), 'pywebhdfs.log')
      level = lg.DEBUG
      
      if 'configuration' in self.config:
        configuration = self.config['configuration']
        if 'logging' in configuration:
          logging_config = configuration['logging']
          if 'disable' in logging_config and logging_config['disable'] == True:
            return NullHandler()
          if 'path' in logging_config:
            path = logging_config['path'] # Override default path.
          if 'level' in logging_config:
            level = getattr(lg, logging_config['level'].upper())

      log_handler = TimedRotatingFileHandler(
          path,
          when='midnight', # Daily backups.
          backupCount=5,
          encoding='utf-8',
      )
      fmt = '%(asctime)s\t%(name)-16s\t%(levelname)-5s\t%(message)s'
      log_handler.setFormatter(lg.Formatter(fmt))
      log_handler.setLevel(level)
      return log_handler

  def get_client(self, cluster_name, auth_mechanism=None, **kwargs):
      """Load WebHDFS client.

      :param cluster_name: The client to look up. If the cluster name does not
        exist and exception will be raised.
      :param kwargs: additional arguments can be used to overwrite or add some 
        parameters defined in the configuration file.
      """

      config_copy = deepcopy(self.config)
      for cluster in config_copy['clusters']:
        if cluster['name'] == cluster_name:
            # remove the name parameter from the 
            del cluster['name']
            
            # get the authentication mechanism to use
            auth_mech = auth_mechanism or cluster['auth_mechanism']
            del cluster['auth_mechanism']

            # set overwrite arguments
            for extra_option in kwargs:
              cluster[extra_option] = kwargs[extra_option]

            return create_client(auth_mechanism=auth_mech, **cluster)

      # the name does not exist
      raise HdfsError('Cluster %s is not defined in configuration file.' % cluster_name)
