import re
from threading import Lock
import logging
import os
import logging as lg

try:
    # Python 3
    from urllib.parse import urlparse
except ImportError:  # pragma: no cover
    # Python 2
    from urlparse import urlparse

_logger = lg.getLogger(__name__)

class HdfsError(Exception):

  """Base error class.
  :param message: Error message.
  :param args: optional Message formatting arguments.
  """

  def __init__(self, message, *args):
    super(HdfsError, self).__init__(message % args if args else message)

class AuthenticationError(HdfsError):
    #403 return code
  def __init__(self, message, *args):
    super(AuthenticationError, self).__init__(message % args if args else message)

class HdfsTimeoutError(HdfsError):
  def __init__(self, message, *args):
    super(HdfsTimeoutError, self).__init__(message % args if args else message)

class ForbiddenRequestError(HdfsError):
    #403 return code
  def __init__(self, message, *args):
    super(ForbiddenRequestError, self).__init__(message % args if args else message)

class StandbyError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(StandbyError, self).__init__(message % args if args else message)

class SecurityError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(SecurityError, self).__init__(message % args if args else message)

class InvalidTokenError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(InvalidTokenError, self).__init__(message % args if args else message)

class RecoveryInProgressError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(RecoveryInProgressError, self).__init__(message % args if args else message)

class AlreadyBeingCreatedError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(AlreadyBeingCreatedException, self).__init__(message % args if args else message)

class HdfsIOError(ForbiddenRequestError):
  def __init__(self, message, *args):
    super(HdfsIOError, self).__init__(message % args if args else message)

class FederationError(HdfsError):
  """Federation error class, is a subclass of HdfsError thrown when not suitable mount point is found.

  :param message: Error message.
  :param args: optional Message formatting arguments.

  """
  def __init__(self, message, *args):
    super(FederationError, self).__init__(message % args if args else message)

class SyncHostsList(object):
  """SyncHostList, is an just the encapsulation of the list of hosts passed to the WebHDFSClient
     this class helps to synchronize the access to the hosts list and provides a way to manage
     failover between namenodes in a HA cluster a thread secure way.

  :param hosts: list of namenodes urls.
  """

  def __init__(self,nameservices):
    self.nameservices = nameservices
    self.lock = Lock()

  def __repr__(self):
    return self.nameservices.__repr__()

  def resolve_hosts_from_path(self,hdfs_path):
    # found out which mount to use for that particular hdfs path
    _logger.debug('Resolve host url for hdfs path %s.', hdfs_path)
    curmount=None
    hosts = None
    for nameservice in self.nameservices:
      for mount in nameservice['mounts']:
        if (hdfs_path.rstrip(os.sep) + os.sep).startswith(mount.rstrip(os.sep) + os.sep):
          if not curmount:
            # first mount
            curmount = mount
            hosts = nameservice['urls']
          else:
            if (mount.rstrip(os.sep) + os.sep).startswith(curmount.rstrip(os.sep) + os.sep):
              # mount is a subdirectory of current mount
              curmount = mount
              hosts =  nameservice['urls']

    if not hosts:
      raise FederationError('Could not resolve any nameservice mount for hdfs path %s".', hdfs_path)

    _logger.debug('Using nameservice %s with mount point %s.', hosts, curmount)
    return hosts

  def get_active_host(self,hdfs_path):
    host=None
    with self.lock:
      hosts=self.resolve_hosts_from_path(hdfs_path)
    return hosts[0]

  def get_host_count(self,hdfs_path):
    return len(self.resolve_hosts_from_path(hdfs_path))

  def switch_active_host(self,url,hdfs_path):
    with self.lock:
      hosts=self.resolve_hosts_from_path(hdfs_path)
      if url != hosts[0]:
        # apparently some other process already switched that namenode 
        return False
      else:
        current_active= hosts.pop(0)
        hosts.append(current_active)
        return True

# This is just to get rid of the annoying requests_kerberos errors logs
class AnnoyingErrorsFilter(logging.Filter):
    def filter(self, record):
      if "requests_kerberos" in record.name and 'Mutual authentication unavailable' in record.msg:
        record.lineno = logging.WARNING
      return True
