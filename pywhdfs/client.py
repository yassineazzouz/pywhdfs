#!/usr/bin/python
# -*- coding: utf-8 -*-
## (c) 2015, Yassine Azzouz <yassine.azzouz@gmail.com>

from .utils import hglob
from .utils.utils import *
from contextlib import contextmanager
from multiprocessing.pool import ThreadPool
from threading import Lock, Semaphore, BoundedSemaphore
from subprocess import call
from getpass import getuser
from shutil import move, rmtree
import requests as rq
import logging as lg
import itertools as it
import glob
import fnmatch
import re
import time
import stat
import grp
import pwd
import sys
import os
import posixpath as psp
import os.path as osp
from datetime import datetime

try:
    # Python 3
    import http.client as httplib
except ImportError:  # pragma: no cover
    # Python 2
    import httplib

try:
    import krbV
    import requests_kerberos
    from requests_kerberos import HTTPKerberosAuth
    KRB_LIB_IMPORT=True
except ImportError:
    KRB_LIB_IMPORT=False

_logger = lg.getLogger(__name__)

webhdfs_prefix = '/webhdfs/v1'

AUTH_MECHANISMS = ['NONE', 'GSSAPI', 'TOKEN', 'TKN_GSSAPI']

def create_client(auth_mechanism, **kwargs):
  if auth_mechanism == "NONE":
    return InsecureWebHDFSClient(**kwargs)
  elif auth_mechanism == "GSSAPI":
    return KrbWebHDFSClient(**kwargs)
  elif auth_mechanism == "TOKEN":
    return TokenWebHDFSClient(**kwargs)
  elif auth_mechanism == "TKN_GSSAPI":
    return KrbTokenWebHDFSClient(**kwargs)
  else:
    raise NotSupportedError(
      'Unsupported authentication mechanism: {0}'.format(auth_mechanism))

class WebHDFSClient(object):
  """Web HDFS client.

  :param nameservices: List of dictionaries specifying the namenodes to connect to, each dict should be
   a list of hostnames or IP addresses of HDFS namenodes (in HA) and a mount point for it
   when using hdfs federation.
   Note: Each host name should be prefixed with protocol and followed by WebHDFS port on namenode.
  :auth_mechanism: Authentication Method to use, one of 'NONE', 'GSSAPI', 'TOKEN'
  :mutual_auth: One of "OPTIONAL", "REQUIRED", "DISABLED"
  :max_concurrency: Max threads to allow for concurent jobs.
  :param user: The username used to connect to the cluster, valid only with NONE authentication.
  :param token: The tocken to use to authenticate when using token authentication.
  :param proxy: User to proxy as.
  :param root: Root path, this will be prefixed to all HDFS paths passed to the
    client. If the root is relative, the path will be assumed relative to the
    user's home directory.
  :param timeout: Connection timeouts, forwarded to the request handler. How
    long to wait for the server to send data before giving up, as a float, or a
    `(connect_timeout, read_timeout)` tuple. If the timeout is reached, an
    appropriate exception will be raised. See the requests_ documentation for
    details.
  :param verify: If the Namenode certificate should be verified or not when using SSL
     Could be a boolean True/False or a path a Truststore file.
  """

  def __init__(
           self,
           nameservices,
           max_concurrency=-1,
           pool_connections=60,
           root=None,
           proxy=None,
           timeout=None,
           verify=False,
           truststore=None,
           session=None
       ):

    # Comma separed list of namenodes urls
    self.host_list = SyncHostsList(nameservices)
    self.root = root

    self.max_fail_retries = 10
    self.on_fail_delay = 6 # Seconds

    self._session = session or rq.Session()
    # Use a bigger connection pool due to the big number of concurrent threads 
    adapter = rq.adapters.HTTPAdapter(max_retries=5, pool_connections=pool_connections, pool_maxsize=pool_connections)
    self._session.mount('http://', adapter)
    self._session.mount('https://', adapter)

    self.max_concurrency = int(max_concurrency)
    if self.max_concurrency > 0:
      self._lock = Lock()
      self._sem = Semaphore(int(self.max_concurrency))
      # ensure there is a least _concurency_delay time difference between
      # two consecutive requests, avoid flooding the namenode/datanode with requests
      self._concurency_delay = 0.001 # Seconds.
      self._timestamp = time.time() - self._concurency_delay

    if proxy:
      if not self._session.params:
        self._session.params = {}
      self._session.params['doas'] = proxy

    self._timeout = timeout
    self.proxy = proxy

    if verify and truststore is not None:
      self._verify = truststore
      _logger.info('Using secure connection with truststore %r', truststore)
    else:
      self._verify = verify

    _logger.info('Instantiated %r.', self)

  def __repr__(self):
    return '<%s(nameservices=%r)>' % (self.__class__.__name__, self.host_list)

  def _api_request(self, method, params, hdfs_path, data=None, strict=True, **rqargs):
    """Wrapper function."""
    max_attemps = self.host_list.get_host_count(hdfs_path)
    attempt = 0

    while True:
      host = self.host_list.get_active_host(hdfs_path)
      url = '%s%s%s' % (
        host.rstrip('/'),
        webhdfs_prefix,
        self.resolvepath(hdfs_path),
      )
      try:
        response = self._request(
          method=method,
          url=url,
          strict=strict,
          data=data,
          params=params,
          **rqargs
        )
        return response
      ## Handle stanby failover
      except StandbyError, e:
        _logger.warn('Namenode %s in standby mode. %s', host, str(e))
        self.host_list.switch_active_host(host,hdfs_path)
        attempt += 1
        if attempt >= max_attemps:
          raise HdfsError('Could not find any active namenode.')
        else:
          pass

    raise HdfsError('Inexpected Process End.')

  '''
    Generic Request handler, do not implement the failover controller for HA here
    since this function is used by redections to data nodes too when doing write
    operation, so this is not specific to namenode requests only.
  '''
  def _request(self, method, url, strict=True, **rqargs):
    _logger.debug('Attempting %s request on url %s with parameters %s', method ,url, rqargs)

    def _on_fail(response, strict=True):

      try:
        message = response.json()['RemoteException']['message']
      except ValueError:
        # No clear one thing to display, display entire message content
        message = response.content
      try:
        exception = str(response.json()["RemoteException"]["exception"])
      except ValueError:
        exception = ""

      if response.status_code == httplib.UNAUTHORIZED:
        _logger.error('Authentication Failure.')
        raise AuthenticationError('Authentication failure. Check your credentials.')

      if response.status_code == httplib.REQUEST_TIMEOUT or response.status_code == httplib.GATEWAY_TIMEOUT:
        _logger.warn('Failed %s request on url %s returned with status %s, Remote Exception : %s, Message: %s', response.request.method ,response.url, response.status_code, str(exception), str(message))
        raise HdfsTimeoutError("TimeoutException : %r",message)

      if response.status_code == httplib.FORBIDDEN:
        _logger.warn('Failed %s request on url %s returned with status %s, Remote Exception : %s, Message: %s', response.request.method ,response.url, response.status_code, str(exception), str(message))
        if exception == "SecurityException":
          _logger.warn('Delegation token expired')
          if "InvalidToken" in message:
            raise InvalidTokenError("InvalidTokenException : %r",message)
          else:
            raise SecurityError("SecurityError : %r",message)
          # else it is something else
        elif exception == "InvalidToken":
          _logger.warn('Delegation token expired')
          raise InvalidTokenError("InvalidTokenException : %r",message)
        elif exception == "StandbyException":
          _logger.warn('Request returned Standby Exception on url %s.', response.url)
          raise StandbyError("StandbyException : %r",message)
        elif exception == "AlreadyBeingCreatedException":
          _logger.warn('Request returned AlreadyBeingCreatedError on url %s.', response.url)
          raise AlreadyBeingCreatedError("AlreadyBeingCreatedError : %r",message)
        elif exception == "RecoveryInProgressException":
          _logger.warn('Request returned RecoveryInProgressError on url %s.', response.url)
          raise RecoveryInProgressError("RecoveryInProgressError : %r",message)
        elif exception == "IOException":
          _logger.warn('Request returned HdfsIOError on url %s.', response.url)
          if "SocketTimeout" in str(message):
             raise HdfsTimeoutError("TimeoutException : %r",message)
          raise HdfsIOError("HdfsIOError : %r",message)
        else:
          # resolve the exception based on message, if the exception
          # is unknown
          if "SocketTimeout" in str(message):
            raise HdfsTimeoutError("TimeoutException : %r",message)
          elif re.match("token .* is expired",str(message)):
            _logger.warn('Delegation token expired')
            raise InvalidTokenError("InvalidTokenException : %r",str(message))
          if strict:
            raise ForbiddenRequestError("ForbiddenException : %r",message)
      if strict:
        _logger.error('Failed %s request on url %s returned with status %s, Remote Exception : %s, Message: %s', response.request.method ,response.url, response.status_code, str(exception), str(message))
        raise HdfsError(message)
      else:
        _logger.debug('Ignoring Remote Exception for %s request on url %s : %s', response.request.method ,response.url, str(message))
        return response

    retries = 0
    while True:
      if self.max_concurrency > 0:
        # Control the number of parallel requests
        with self._sem:
          with self._lock:
            # the current time need to exceed the last query time + a delay
            delay = self._timestamp + self._concurency_delay - time.time()
            if delay > 0:
              time.sleep(delay) # Avoid replay errors.
              self._timestamp = time.time() # last request time
          response = self._session.request(
            method=method,
            url=url,
            timeout=self._timeout,
            verify=self._verify,
            headers={'content-type': 'application/octet-stream'},
            **rqargs
          )
      else:
        response = self._session.request(
          method=method,
          url=url,
          timeout=self._timeout,
          verify=self._verify,
          headers={'content-type': 'application/octet-stream'},
          **rqargs
        )

      # returns True if status_code is less than 400
      if not response:
        try:
          return _on_fail(response=response,strict=strict)
        # recoverable errors
        except (HdfsTimeoutError, RecoveryInProgressError, AlreadyBeingCreatedError, HdfsIOError) as e:
          _logger.warn('Request failed %s', str(e))
          retries += 1
          if retries >= self.max_fail_retries:
            raise HdfsError('Exceeded maximum number of retries after %s attemps, failing.')
          else:
            _logger.warn('Retrying failed request, attempt %s of %s', retries, self.max_fail_retries)
            time.sleep(self.on_fail_delay)
            pass
      else:
        _logger.debug('%s request on url %s returned with status %s', method ,url, response.status_code)
        return response

  def help(self):
    help(self)

  def get_current_user(self):
      if self.proxy:
        return self.proxy
      else:
        return get_authenticated_user()

  def get_authenticated_user(self):
      return getuser()  

  def content(self, hdfs_path, strict=True):
    """Get ContentSummary_ for a file or folder on HDFS.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    .. _ContentSummary: CS_
    .. _CS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#ContentSummary
    """
    _logger.debug('Fetching content summary for %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'GETCONTENTSUMMARY'}, strict=strict)
    return res.json()['ContentSummary'] if res else None

  def status(self, hdfs_path, strict=True):
    """Get FileStatus_ for a file or folder on HDFS.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    .. _FileStatus: FS_
    .. _FS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus
    """
    _logger.debug('Fetching status for %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'GETFILESTATUS'}, strict=strict)
    return res.json()['FileStatus'] if res else None


  def delete(self, hdfs_path, recursive=False):
    """Remove a file or directory from HDFS.
    :param hdfs_path: HDFS path.
    :param recursive: Recursively delete files and directories. By default,
      this method will raise an :class:`HdfsError` if trying to delete a
      non-empty directory.
    This function returns `True` if the deletion was successful and `False` if
    no file or directory previously existed at `hdfs_path`.
    """
    _logger.debug(
      'Deleting %r%s.', hdfs_path, ' recursively' if recursive else ''
    )
    return self._api_request(method='DELETE', hdfs_path=hdfs_path, params={'op': 'DELETE', 'recursive': recursive}).json()['boolean']

  def rename(self, hdfs_src_path, hdfs_dst_path):
    """Move a file or folder.
    :param hdfs_src_path: Source path.
    :param hdfs_dst_path: Destination path. If the path already exists and is
      a directory, the source will be moved into it. If the path exists and is
      a file, or if a parent destination directory is missing, this method will
      raise an :class:`HdfsError`.
    """
    _logger.debug('Renaming %r to %r.', hdfs_src_path, hdfs_dst_path)
    hdfs_dst_path = self.resolvepath(hdfs_dst_path)
    res = self._api_request(method='PUT', hdfs_path=hdfs_src_path, params={'op': 'RENAME', 'destination': hdfs_dst_path})
    if not res.json()['boolean']:
      raise HdfsError(
        'Unable to rename %r to %r.',
        self.resolvepath(hdfs_src_path), hdfs_dst_path
      )

  def set_owner(self, hdfs_path, owner=None, group=None):
    """Change the owner of file.
    :param hdfs_path: HDFS path.
    :param owner: Optional, new owner for file.
    :param group: Optional, new group for file.
    At least one of `owner` and `group` must be specified.
    """
    if not owner and not group:
      raise ValueError('Must set at least one of owner or group.')
    messages = []
    if owner:
      messages.append('owner to %r' % (owner, ))
    if group:
      messages.append('group to %r' % (group, ))
    _logger.debug('Changing %s of %r.', ', and'.join(messages), hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, params={'op': 'SETOWNER', 'owner': owner, 'group': group})

  def set_permission(self, hdfs_path, permission):
    """Change the permissions of file.
    :param hdfs_path: HDFS path.
    :param permission: New octal permissions string of file.
    """
    _logger.debug(
      'Changing permissions of %r to %r.', hdfs_path, permission
    )
    self._api_request(method='PUT', hdfs_path=hdfs_path, params={'op': 'SETPERMISSION', 'permission': permission})

  def set_times(self, hdfs_path, access_time=None, modification_time=None):
    """Change remote timestamps.
    :param hdfs_path: HDFS path.
    :param access_time: Timestamp of last file access.
    :param modification_time: Timestamps of last file access.
    """
    if not access_time and not modification_time:
      raise ValueError('At least one of time must be specified.')
    msgs = []
    if access_time:
      msgs.append('access time to %r' % (access_time, ))
    if modification_time:
      msgs.append('modification time to %r' % (modification_time, ))
    _logger.debug('Updating %s of %r.', ' and '.join(msgs), hdfs_path)
    self._api_request(method='PUT',hdfs_path=hdfs_path, params={'op': 'SETTIMES', 'accesstime': access_time, 'modificationtime': modification_time})

  def set_replication(self, hdfs_path, replication):
    """Set file replication.
    :param hdfs_path: Path to an existing remote file. An :class:`HdfsError`
      will be raised if the path doesn't exist or points to a directory.
    :param replication: Replication factor.
    """
    _logger.debug(
      'Setting replication factor to %r for %r.', replication, hdfs_path
    )
    res = self._api_request(method='PUT',hdfs_path=hdfs_path, params={'op': 'SETREPLICATION', 'replication': replication})
    if not res.json()['boolean']:
      raise HdfsError('%r is not a file.', hdfs_path)

  def makedirs(self, hdfs_path, permission=None):
    """Create a remote directory, recursively if necessary.
    :param hdfs_path: Remote path. Intermediate directories will be created
      appropriately.
    :param permission: Octal permission to set on the newly created directory.
      These permissions will only be set on directories that do not already
      exist.
    This function currently has no return value as WebHDFS doesn't return a
    meaningful flag.
    """
    _logger.debug('Creating directories to %r.', hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, params={'op': 'MKDIRS', 'permission': permission})

  def checksum(self, hdfs_path):
    """Get a remote file's checksum.
    :param hdfs_path: Remote path. Must point to a file.
    """
    _logger.debug('Getting checksum for %r.', hdfs_path)
    return self._api_request(method='GET',hdfs_path=hdfs_path, params={'op': 'GETFILECHECKSUM'}).json()['FileChecksum']

  def list(self, hdfs_path, status=False, glob=False):
    """Return names of files contained in a remote folder.
    :param hdfs_path: Remote path to a directory. If `hdfs_path` doesn't exist
      or points to a normal file, an :class:`HdfsError` will be raised.
    :glob: Whether the path should be considered a glob expressions
    :param status: Also return each file's corresponding FileStatus.
    """
    _logger.debug('Listing %r.', hdfs_path)
    hdfs_path = self.resolvepath(hdfs_path)

    if not glob:
      statuses = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'LISTSTATUS'}).json()['FileStatuses']['FileStatus']
      if len(statuses) == 1 and (
        not statuses[0]['pathSuffix'] or self.status(hdfs_path)['type'] == 'FILE'
        # HttpFS behaves incorrectly here, we sometimes need an extra call to
        # make sure we always identify if we are dealing with a file.
      ):
        raise HdfsError('%r is not a directory.', hdfs_path)
      if status:
        return [(s['pathSuffix'], s) for s in statuses]
      else:
        return [s['pathSuffix'] for s in statuses]
    else:
      files = [ hdfs_file for hdfs_file in hglob.iglob(self, hdfs_path) ]
      if status:
        return [(f, self.status(f)) for f in files]
      else:
        return files

  def walk(self, hdfs_path, depth=0, status=False):
    """Depth-first walk of remote filesystem.
    :param hdfs_path: Starting path. If the path doesn't exist, an
      :class:`HdfsError` will be raised. If it points to a file, the returned
      generator will be empty.
    :param depth: Maximum depth to explore. `0` for no limit.
    :param status: Also return each file or folder's corresponding FileStatus_.
    This method returns a generator yielding tuples `(path, dirs, files)`
    where `path` is the absolute path to the current directory, `dirs` is the
    list of directory names it contains, and `files` is the list of file names
    it contains.
    """
    _logger.debug('Walking %r (depth %r).', hdfs_path, depth)

    def _walk(dir_path, dir_status, depth):
      """Recursion helper."""
      infos = self.list(dir_path, status=True)
      dir_infos = [info for info in infos if info[1]['type'] == 'DIRECTORY']
      file_infos = [info for info in infos if info[1]['type'] == 'FILE']
      if status:
        yield ((dir_path, dir_status), dir_infos, file_infos)
      else:
        yield (
          dir_path,
          [name for name, _ in dir_infos],
          [name for name, _ in file_infos],
        )
      if depth != 1:
        for name, s in dir_infos:
          path = psp.join(dir_path, name)
          for infos in _walk(path, s, depth - 1):
            yield infos

    hdfs_path = self.resolvepath(hdfs_path) # Cache resolution.
    s = self.status(hdfs_path)
    if s['type'] == 'DIRECTORY':
      for infos in _walk(hdfs_path, s, depth):
        yield infos

  ###     Snapshot Functions     ###

  def create_snapshot(self, hdfs_path, snapshotname=None):
    """Create a Snapshot on a particular HDFS directory.
    :param hdfs_path: HDFS path to snapshot.
    :param snapshotname: Name of the snapshot.
    """
    if snapshotname != None:
      _logger.debug('Creating a snapshot %r on hdfs path %r.', snapshotname, hdfs_path)
    else:
      _logger.debug('Creating a snapshot on hdfs path %r.', hdfs_path)
    res = self._api_request(method='PUT',hdfs_path=hdfs_path, params={'op': 'CREATESNAPSHOT', 'snapshotname': snapshotname})
    _logger.debug('Successfully Created snapshot for hdfs path %r on %r.', hdfs_path, res.json()['Path'])
    return res.json()['Path']

  def delete_snapshot(self, hdfs_path, snapshotname):
    """Delete a Snapshot on a particular HDFS directory.
    :param hdfs_path: HDFS path to snapshot.
    :param snapshotname: Name of the snapshot to delete.
    """
    _logger.debug('Deleting snapshot %r on hdfs path %r.', snapshotname, hdfs_path)
    self._api_request(method='DELETE', hdfs_path=hdfs_path, params={'op': 'DELETESNAPSHOT', 'snapshotname': snapshotname})
    _logger.debug('Successfully Deleted snapshot %r.', snapshotname)

  def rename_snapshot(self, hdfs_path, oldsnapshotname, snapshotname):
    """Create a Snapshot on a particular HDFS directory.
    :param hdfs_path: HDFS path to snapshot.
    :param oldsnapshotname: Name of the snapshot to rename.
    :param snapshotname: New snapshot name.
    """
    _logger.debug('Renaming snapshot %r on hdfs path %r to %r.', oldsnapshotname, hdfs_path, snapshotname)
    self._api_request(method='PUT',  hdfs_path=hdfs_path, params={'op': 'RENAMESNAPSHOT', 'oldsnapshotname': oldsnapshotname, 'snapshotname': snapshotname})
    _logger.debug('Successfully Renamed snapshot %r to %r.', oldsnapshotname, snapshotname)

  def list_snapshots(self, path):
    snapshots_path = self.resolvepath(path) + "/.snapshot"
    status = self.status(hdfs_path=path,strict=False)
    if status == None or status['type'] != 'DIRECTORY':
      raise HdfsError('%r is not a valid snapshottable directory".', hdfs_path)
    res = self.list(hdfs_path=snapshots_path)
    return res

  ### Extended Attributes(XAttrs) Functions ###

  def getxattrs(self, hdfs_path, key=None, encoding="text", strict=True):
    """Get extended attributes on a particular path.
    :param hdfs_path: Remote path.
    :param key: The attribute name to get the value for, if None return all attributes.
    :param encoding: The XAttr value encoding, Valid Values are 'text','hex' or 'base64'.
    :param strict: If `False`, return `None` rather than raise an exception if the path doesn't exist.
    :returns a list of extended attribute dictionnairies.
    """
    if key != None:
      _logger.debug('Fetching extended attribute %r status for %r.', key, hdfs_path)
      res = self._api_request(method='GET', hdfs_path=hdfs_path, strict=strict, params={'op': 'GETXATTRS', 'encoding': encoding, 'xattr.name' : key})
    else:
      _logger.debug('Fetching all extended attributes for %r.', hdfs_path)
      res = self._api_request(method='GET', hdfs_path=hdfs_path, strict=strict, params={'op': 'GETXATTRS', 'encoding': encoding})
    if res:
      xattrs = {}
      for ent in res.json()['XAttrs']:
        xattrs.update({ ent['name'] : ent['value'] })
    else:
      xattrs=None
    return xattrs

  def listxattrs(self, hdfs_path, strict=True):
    """List all existing extended attributes on a particular path.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if the path doesn't exist.
    """
    _logger.debug('Listing all extended attributes for %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, strict=strict, params={'op': 'LISTXATTRS'})
    return res.json()['XAttrNames'] if res else None

  def removexattr(self, hdfs_path, key, strict=False):
    """Remove an extended attribute.
    :param hdfs_path: Remote path.
    :param key: the name of the extended attribute to delete.
    :param strict: If `False`, does not fail if the attribute is not defined on that path.
    """
    _logger.debug('Removing extended attribute %r for path %r.', key , hdfs_path)
    res = self._api_request(method='PUT', hdfs_path=hdfs_path, strict=strict, params={'op': 'REMOVEXATTR', 'xattr.name' : key})
    return True if res else False

  def setxattr(self, hdfs_path, key, value, overwrite=True):
    """Set or update an extended attribute.
    :param hdfs_path: Remote path.
    :param key: extended attribute name.
    :param value: extended attribute value.
    :param overwrite: If `False`, does not fail if the attribute is already defined on that path.
    """
    _logger.debug('Setting extended attribute %r for path %r.', key , hdfs_path)
    try:
      self._api_request(method='PUT', hdfs_path=hdfs_path,  params={'op': 'SETXATTR','flag': 'CREATE', 'xattr.name' : key, 'xattr.value': value})
    except HdfsError, e:
      if "already exists" in str(e):
        _logger.debug('Looks like extended attribute %r already exist for path %r.', key , hdfs_path)
        if overwrite:
          try:
            _logger.debug('Replacing extended attribute %r value on path %r.', key , hdfs_path)
            self._api_request(method='PUT', hdfs_path=hdfs_path, params={'op': 'SETXATTR','flag': 'REPLACE', 'xattr.name' : key, 'xattr.value': value})
          except Exception, e:
            raise e
        else:
          raise e
      else:
        raise e
    except Exception, e:
      raise e
    return True

  ### Delegation Token Functions ###
  def cancelDelegationToken(self, token):
    """Cancel an existing delegation token.
    :param hdfs_path: Remote path.
    :param token: delegation token obtained earlier.
    """
    _logger.debug('Canceling Delegation Token %r.', token)
    self._api_request(method='PUT', hdfs_path="/", params={'op': 'CANCELDELEGATIONTOKEN', 'token': token})

  def renewDelegationToken(self, token):
    """Renew an existing delegation token.
    :param hdfs_path: Remote path.
    :param token: delegation token obtained earlier.
    :returns: the new expiration time. ex: {"long":1450171469608}
    """
    _logger.debug('Renewing Delegation Token.')
    res = self._api_request(method='PUT', hdfs_path="/", params={'op': 'RENEWDELEGATIONTOKEN', 'token': token})
    return res.json()['long'] if res else None

  def listDelegationTokens(self, renewer):
    """Get one or more delegation tokens associated with the filesystem.
       Normally a file system returns a single delegation token.
       A file system that manages multiple file systems underneath,
       could return set of delegation tokens for all the file systems it manages.
    :param hdfs_path: Remote path.
    :param renewer: The account name that is allowed to renew the token.
    """
    _logger.debug('Listing Delegation Token for renewer %r.', renewer)
    try:
      res = self._api_request(method='GET', hdfs_path="/", params={'op': 'GETDELEGATIONTOKENS', 'renewer': renewer})
      return res.json()['Tokens'] if res else None
    except HdfsError, e:
      if "Invalid value for webhdfs parameter" in str(e):
        _logger.error('There is a webhdfs bug in GETDELEGATIONTOKENS operation, hopefully this will be fixed in future releases.')
        return None
      else:
        raise e
    except Exception, e:
      raise e

  def getDelegationToken(self, renewer, service=None, kind=None):
    """Get a new delegation token for this file system.
    :param hdfs_path: Remote path.
    :param renewer:  Name of the designated renewer for the token.
    :param service: the service for this token
    :param kind: the kind of token
    """
    _logger.debug('Requesting Delegation Token for renewer %r.', renewer)
    res = self._api_request(method='GET', hdfs_path="/", params={'op': 'GETDELEGATIONTOKEN', 'service': service, 'kind': kind})
    return res.json()['Token'] if res else None

  ### File System Access Functions ###
  def checkAccess(self, hdfs_path, fsaction, strict=True):
    """Checks if the user can access a path. The mode specifies which access checks to perform.
       If the requested permissions are granted, then the method returns true.
       If access is denied, then the method throws false.
    :param hdfs_path: Remote path.
    :param fsaction: type of access to check
      Enum { ALL ,EXECUTE ,NONE ,READ ,READ_EXECUTE ,READ_WRITE ,WRITE ,WRITE_EXECUTE }
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    :returns: boolean
    """
    _logger.debug('Checking access for %r.', hdfs_path)
    access = True
    try:
      res = self._api_request(method='GET', hdfs_path=hdfs_path, strict=strict, params={'op': 'CHECKACCESS'})
    except HdfsError, e:
      if 'Permission denied:' %path in str(e):
        access = False
      else:
        raise e
    return access

  def getAclStatus(self, hdfs_path, strict=True):
    """Get getAclStatus return the ACLS for a file or folder on HDFS.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    """
    _logger.debug('Fetching ACL status for %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, strict=strict, params={'op': 'GETACLSTATUS'})
    return res.json()['AclStatus'] if res else None

  def setAcl(self, hdfs_path, aclspec):
    """Fully replaces ACL of files and directories, discarding all existing entries.
    :param hdfs_path: Remote path.
    :param aclspec: a comma List describing modifications,
      must include entries for user, group, and others for compatibility with permission bits.
      example: user::rwx,group::r--,other::---.
    """
    _logger.debug('Replacing ACLS %r for %r.', aclspec, hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, params={'op': 'SETACL', 'aclspec': aclspec})

  def setacls(self, path, entries, recursive=False, strict=False):

    # Fail if file does not exist
    status = self.status(hdfs_path=path,strict=strict)

    if status is None:
      return False

    if recursive:
      proc_raw_entries = ""
      for raw_ent in entries:
        if raw_ent[0] != 'default':
          if proc_raw_entries != "":
            proc_raw_entries = proc_raw_entries + "," + raw_ent
          else:
            proc_raw_entries = raw_ent

      if status['type'] == 'DIRECTORY':
        for root, dirs, files in self.walk(path, depth=0, status=False):
          for f in files:
            fpath = os.path.join(root, f)
            # for files remove default acls
            self.setAcl(hdfs_path=fpath, aclspec=proc_raw_entries)
          for d in dirs:
            dpath = os.path.join(root, d)
            self.setAcl(hdfs_path=dpath, aclspec=entries)
      else:
        self.setAcl(hdfs_path=path, aclspec=proc_raw_entries)
    else:
      # non recursive
      self.setAcl(hdfs_path=dpath, aclspec=entries)

    return True

  def removeAcl(self, hdfs_path, strict=True):
    """Removes all but the base ACL entries of files and directories.
       The entries for user, group, and others are retained for compatibility with permission bits.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    """
    _logger.debug('Removing ACLS for %r.', hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, strict=strict, params={'op': 'REMOVEACL'})

  def removeDefaultAcl(self, hdfs_path, strict=True):
    """Removes all default ACL entries from files and directories.
    :param hdfs_path: Remote path.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    """
    _logger.debug('Removing default ACLS for %r.', hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, strict=strict, params={'op': 'REMOVEDEFAULTACL'})

  def removeAclEntries(self, hdfs_path, aclspec, strict=True):
    """Removes ACL entries from files and directories. Other ACL entries are retained.
    :param hdfs_path: Remote path.
    :param aclspec: a comma List describing entries to remove.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    """
    _logger.debug('Removing ACLS entry %r for %r.', aclspec, hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, strict=strict, params={'op': 'REMOVEACLENTRIES', 'aclspec': aclspec})

  def modifyAclEntries(self, hdfs_path, aclspec, strict=True):
    """Modifies ACL entries of files and directories.
       This method can add new ACL entries or modify the permissions on existing ACL entries.
       All existing ACL entries that are not specified in this call are retained without changes.
       (Modifications are merged into the current ACL.)
    :param hdfs_path: Remote path.
    :param aclspec: a comma List describing modifications.
    :param strict: If `False`, return `None` rather than raise an exception if
      the path doesn't exist.
    """
    _logger.debug('Replacing ACLS %r for %r.', aclspec, hdfs_path)
    self._api_request(method='PUT', hdfs_path=hdfs_path, strict=strict, params={'op': 'MODIFYACLENTRIES', 'aclspec': aclspec})


  def download(self, hdfs_path, local_path, overwrite=False, n_threads=1, preserve=False,
    temp_dir=None, **kwargs):
    """Download a file or folder from HDFS and save it locally.

    :param hdfs_path: Path on HDFS of the file or folder to download. If a
      folder, all the files under it will be downloaded.
    :param local_path: Local path. If it already exists and is a directory,
      the files will be downloaded inside of it.
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallelization. A value of
      `0` (or negative) uses as many threads as there are files.
    :param temp_dir: Directory under which the files will first be downloaded
      when `overwrite=True` and the final destination path already exists. Once
      the download successfully completes, it will be swapped in.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`read`. If no
      `chunk_size` argument is passed, a default value of 64 kB will be used.
      If a `progress` argument is passed and threading is used, care must be
      taken to ensure correct behavior.

    On success, this method returns the local download path.

    """
    start_time = time.time()
    _logger.info('Downloading %r to %r.', hdfs_path, local_path)
    kwargs.setdefault('chunk_size', 2 ** 16)
    lock = Lock()

    def _download(_path_tuple):
      """Download a single file."""
      _remote_path, _temp_path = _path_tuple
      _logger.debug('Downloading %r to %r.', _remote_path, _temp_path)
      _dpath = osp.dirname(_temp_path)
      with lock:
        # Prevent race condition when using multiple threads.
        if not osp.exists(_dpath):
          os.makedirs(_dpath)
      with open(_temp_path, 'wb') as _writer:
        with self.read(_remote_path, **kwargs) as reader:
          for chunk in reader:
            _writer.write(chunk)

    # Normalise local and remote paths and turn relative paths into absolute paths
    hdfs_path = self.resolvepath( hdfs_path )
    local_path = osp.realpath( osp.normpath(local_path) )

    # First, resolve the list of local files/directories to be uploaded
    downloads = [ upload_file for upload_file in hglob.iglob(self, hdfs_path) ]

    # Second, figure out where we will download the files to.
    tuples = []
    for download in downloads:
      download_tuple = dict()
      if osp.exists(local_path):
        # local destination path exist
        if osp.isfile(local_path):
          # local dest path exist and is a normal file
          if not overwrite:
            raise HdfsError('Path %r already exists.', local_path)
          local_base_path = local_path
          local_dpath, local_name = osp.split(local_base_path)
          temp_dir = temp_dir or local_dpath
          temp_path = osp.join(
            temp_dir,
            '%s.temp-%s' % (local_name, int(time.time()))
          )
          _logger.debug(
            'Download destination %r already exists. Using temporary path %r.',
            local_base_path, temp_path
          )

        elif osp.isdir(local_path):
          # lacal path exists and is a directory
          if osp.exists( osp.join( local_path, osp.basename(download) ) ):
            local_base_path =  osp.join( local_path, osp.basename(download) )
            if not overwrite:
              raise HdfsError('Local path %r already exists.', local_base_path)
            local_dpath, local_name = osp.split(local_base_path)
            temp_dir = temp_dir or local_dpath
            temp_path = osp.join(
              temp_dir,
              '%s.temp-%s' % (local_name, int(time.time()))
            )
            _logger.debug(
              'Download destination %r already exists. Using temporary path %r.',
              local_base_path, temp_path
            )
          else:
            local_base_path =  osp.join( local_path, osp.basename(download) )
            temp_path = local_base_path

        download_tuple = dict({ 'local_path' : local_base_path, 'hdfs_path' : download, 'temp_path' : temp_path})
      else:
        # local destination path does not exist
        if not osp.exists(osp.dirname(local_path)):
          raise HdfsError('Parent directory of %r does not exist.', local_path)
        local_base_path =  local_path
        temp_path = local_base_path
        download_tuple = dict({ 'local_path' : local_base_path, 'hdfs_path' : download, 'temp_path' : local_base_path})

      # add download tuple to the array
      tuples.append(download_tuple)


    fpath_tuples = []
    for download_tuple in tuples:
      # Then we figure out which files we need to download and where.
      remote_paths = list(self.walk(download_tuple['hdfs_path']))
      if not remote_paths:
        # This is a single file.
        remote_fpaths = [download_tuple['hdfs_path']]
      else:
        remote_fpaths = [
          osp.join(dpath, fname)
          for dpath, _, fnames in remote_paths
          for fname in fnames
        ]
      offset = len(download_tuple['hdfs_path']) + 1 # Prefix length.
      fpath_tuples = [
        (
          fpath,
          osp.join(download_tuple['temp_path'], fpath[offset:].replace('/', os.sep)).rstrip(os.sep)
        )
        for fpath in remote_fpaths
      ]

    # Finally, we download all of them.
    if n_threads <= 0:
      n_threads = len(fpath_tuples)
    else:
      n_threads = min(n_threads, len(fpath_tuples))
    _logger.debug(
      'Downloading %s files using %s thread(s).', len(fpath_tuples), n_threads
    )
    try:
      if n_threads == 1:
        for fpath_tuple in fpath_tuples:
          _download(fpath_tuple)
      else:
        _map_async(n_threads, _download, fpath_tuples)
    except Exception as err: # pylint: disable=broad-except
      _logger.exception('Error while downloading. Attempting cleanup.')
      try:
        for download_tuple in tuples:
          if osp.isdir(download_tuple['temp_path']):
            rmtree(download_tuple['temp_path'])
          else:
            os.remove(download_tuple['temp_path'])
      except Exception:
        _logger.error('Unable to cleanup temporary folder.')
      finally:
        raise err
    else:
      for download_tuple in tuples:
        if download_tuple['temp_path'] != download_tuple['local_path']:
          _logger.debug(
            'Download of %r complete. Moving from %r to %r.',
            download_tuple['hdfs_path'], download_tuple['temp_path'], download_tuple['local_path']
          )
          if osp.isdir(download_tuple['local_path']):
            rmtree(download_tuple['local_path'])
          else:
            os.remove(download_tuple['local_path'])
          move(download_tuple['temp_path'], download_tuple['local_path'])
        else:
          _logger.debug(
            'Download of %s to %r complete.', download_tuple['hdfs_path'], download_tuple['local_path']
          )

    _logger.debug("--- download finished in : %s seconds ---" % (time.time() - start_time))
    return local_path

  def read_file(self, hdfs_path, offset=0, length=None, buffer_size=None, encoding=None):
    _logger.debug('Reading file %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'OPEN', 'offset': offset, 'length': length, 'buffersize': buffer_size})
    try:
      res.encoding = encoding
      return res.content
    finally:
      res.close()
      _logger.debug('Closed response for reading file %r.', hdfs_path)

  def read_stream(self, hdfs_path, offset=0, length=None, buffer_size=None, encoding=None, chunk_size=1024, delimiter=None):
    """Stream a file from HDFS.
       This function is a generator that returns chunks of data of chunk_size.
    """
    if delimiter:
      if not encoding:
        raise ValueError('Delimiter splitting requires an encoding.')
      if chunk_size:
        raise ValueError('Delimiter splitting incompatible with chunk size.')
    _logger.info('Reading file %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'OPEN', 'offset': offset, 'length': length, 'buffersize': buffer_size}, stream=True)
    try:
      res.encoding = encoding
      if delimiter:
        for chunk in res.iter_lines(delimiter=delimiter, decode_unicode=True):
          if chunk:
            yield chunk
      else:
        for chunk in res.iter_content(chunk_size=chunk_size, decode_unicode=True):
          if chunk:
            yield chunk
    finally:
      res.close()
      _logger.debug('Closed response for reading file %r.', hdfs_path)

  @contextmanager
  def read(self, hdfs_path, offset=0, length=None, buffer_size=None,
    encoding=None, chunk_size=None, delimiter=None, progress=None):
    """Read a file from HDFS.
    :param hdfs_path: HDFS path.
    :param offset: Starting byte position.
    :param length: Number of bytes to be processed. `None` will read the entire
      file.
    :param buffer_size: Size of the buffer in bytes used for transferring the
      data. Defaults the the value set in the HDFS configuration.
    :param encoding: Encoding used to decode the request. By default the raw
      data is returned. This is mostly helpful in python 3, for example to
      deserialize JSON data (as the decoder expects unicode).
    :param chunk_size: If set to a positive number, the context manager will
      return a generator yielding every `chunk_size` bytes instead of a
      file-like object (unless `delimiter` is also set, see below).
    :param delimiter: If set, the context manager will return a generator
      yielding each time the delimiter is encountered. This parameter requires
      the `encoding` to be specified.
    This method must be called using a `with` block:
    .. code-block:: python
      with client.read('foo') as reader:
        content = reader.read()
    This ensures that connections are always properly closed.
    """
    if progress and not chunk_size:
      raise ValueError('Progress callback requires a positive chunk size.')
    if delimiter:
      if not encoding:
        raise ValueError('Delimiter splitting requires an encoding.')
      if chunk_size:
        raise ValueError('Delimiter splitting incompatible with chunk size.')
    _logger.debug('Reading file %r.', hdfs_path)
    res = self._api_request(method='GET', hdfs_path=hdfs_path, params={'op': 'OPEN', 'offset': offset, 'length': length, 'buffersize': buffer_size}, stream=True)
    try:
      if not chunk_size and not delimiter:
        yield codecs.getreader(encoding)(res.raw) if encoding else res.raw
      else:
        res.encoding = encoding
        if delimiter:
          data = res.iter_lines(delimiter=delimiter, decode_unicode=True)
        else:
          data = res.iter_content(chunk_size=chunk_size, decode_unicode=True)
        if progress:
          def reader(_hdfs_path, _progress):
            """Generator that tracks progress."""
            nbytes = 0
            for chunk in data:
              nbytes += len(chunk)
              _progress(_hdfs_path, nbytes)
              yield chunk
            _progress(_hdfs_path, -1)

          yield reader(hdfs_path, progress)
        else:
          yield data
    finally:
      res.close()
      _logger.debug('Closed response for reading file %r.', hdfs_path)

  def write(self, hdfs_path, data, overwrite=False, permission=None,
    blocksize=None, replication=None, buffersize=None, append=False,
    encoding=None):
    """Create a file on HDFS.
    :param hdfs_path: Path where to create file. The necessary directories will
      be created appropriately.
    :param data: Contents of file to write. Can be a string, a generator or a
      file object. The last two options will allow streaming upload (i.e.
      without having to load the entire contents into memory). If `None`, this
      method will return a file-like object and should be called using a `with`
      block (see below for examples).
    :param overwrite: Overwrite any existing file or directory.
    :param permission: Octal permission to set on the newly created file.
      Leading zeros may be omitted.
    :param blocksize: Block size of the file.
    :param replication: Number of replications of the file.
    :param buffersize: Size of upload buffer.
    :param append: Append to a file rather than create a new one.
    :param encoding: Encoding used to serialize data written.
    Sample usages:
    .. code-block:: python
      from json import dump, dumps
      records = [
        {'name': 'foo', 'weight': 1},
        {'name': 'bar', 'weight': 2},
      ]
      # As a context manager:
      with client.write('data/records.jsonl', encoding='utf-8') as writer:
        dump(records, writer)
      # Or, passing in a generator directly:
      client.write('data/records.jsonl', data=dumps(records), encoding='utf-8')
    """
    if append:
      if overwrite:
        raise ValueError('Cannot both overwrite and append.')
      if permission or blocksize or replication:
        raise ValueError('Cannot change file properties while appending.')
      _logger.debug('Appending to %r.', hdfs_path)
      res = self._api_request(method='POST', hdfs_path=hdfs_path, params={'op': 'APPEND', 'buffersize': buffersize}, allow_redirects=False)
    else:
      _logger.debug('Writing to %r.', hdfs_path)
      res = self._api_request(
                    method='PUT',
                    hdfs_path=hdfs_path,
                    params={
                      'op': 'CREATE',
                      'overwrite': overwrite,
                      'permission': permission,
                      'blocksize': blocksize,
                      'replication': replication,
                      'buffersize': buffersize
                    },
                    allow_redirects=False,
                    )

    """
       Handle datanodes redirections :
       Submit another HTTP PUT request using the URL in
       the Location header with the file data to be written.
       Note: This is not an API call do not use the _api_request.
    """
    self._request(
        method='POST' if append else 'PUT',
        url=res.headers['location'],
        data=(c.encode(encoding) for c in data) if encoding else data,
    )

  def upload(self, hdfs_path, local_path, overwrite=False, n_threads=1, preserve=False, checksum=True,
    temp_dir=None, chunk_size=2 ** 16, progress=None, include_pattern="*", files_only=False, min_size=0, **kwargs):
    """Upload a file or directory to HDFS.

    :param hdfs_path: Target HDFS path. If it already exists and is a
      directory, files will be uploaded inside.
    :param local_path: Local path to file or folder. If a folder, all the files
      inside of it will be uploaded (note that this implies that folders empty
      of files will not be created remotely), when pattern is used this will act
      as a root path.
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallelization. A value of
      `0` (or negative) uses as many threads as there are files.
    :param temp_dir: Directory under which the files will first be uploaded
      when `overwrite=True` and the final remote path already exists. Once the
      upload successfully completes, it will be swapped in.
    :param chunk_size: Interval in bytes by which the files will be uploaded.
    :param progress: Callback function to track progress, called every
      `chunk_size` bytes. It will be passed two arguments, the path to the
      file being uploaded and the number of bytes transferred so far. On
      completion, it will be called once with `-1` as second argument.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`write`.

    On success, this method returns the remote upload path.

    """
    start_time = time.time()
    if not chunk_size:
      raise ValueError('Upload chunk size must be positive.')

    lock = Lock()
    dircache = []

    _logger.info('Uploading %r to %r.', local_path, hdfs_path)

    def _preserve(_local_path, _hdfs_path):
      # set the base path attributes
      localstat = os.stat(_local_path)
      _logger.debug("Preserving %r local attributes on %r" % (_local_path,_hdfs_path))
      self.set_owner(_hdfs_path, owner=pwd.getpwuid(localstat.st_uid).pw_name, group=grp.getgrgid(localstat.st_gid).gr_name)
      self.set_permission(_hdfs_path, permission=oct(stat.S_IMODE(localstat.st_mode)))
      self.set_times( _hdfs_path, access_time=int(localstat.st_atime * 1000), modification_time=int(localstat.st_mtime  * 1000))

    def _upload_wrap(_path_tuple):
      _local_path, _hdfs_path = _path_tuple
      try:
        status = _upload(_path_tuple)
        return status
      except Exception as exp:
        _logger.exception('Error while uploading %r to %r. %s' % (_local_path,_hdfs_path,exp))
        return { 'status': 'failed', 'src_path': _local_path, 'dest_path' : _hdfs_path }

    def _upload(_path_tuple):
      """Upload a single file."""

      def wrap(_reader, _chunk_size, _progress):
        """Generator that can track progress."""
        nbytes = 0
        while True:
          chunk = _reader.read(_chunk_size)
          if chunk:
            if _progress:
              nbytes += len(chunk)
              _progress(_local_path, nbytes)
            yield chunk
          else:
            break
        if _progress:
          _progress(_local_path, -1)

      _local_path, _hdfs_path = _path_tuple
      _tmp_path = ""

      skip=False

      dst_st=self.status(_hdfs_path,strict=False)

      if dst_st == None:
        # destination does not exist
        _tmp_path=_hdfs_path
      else:
        # destination exist
        if not overwrite:
          raise HdfsError('Destination file exist and Missing overwrite parameter.')
        _tmp_path = '%s.temp-%s' % (_hdfs_path, int(time.time()))

        if checksum == True:
          _local_path_size = osp.getsize(_local_path)
          _hdfs_path_size = self.content(_hdfs_path)['length']
          if int(_local_path_size) == int(_hdfs_path_size):
            _logger.info('source %r and destination %r seems to be identical, skipping.', _local_path, _hdfs_path)
            skip=True
          else:
            _logger.debug('source destination files does not seems to have the same checksum value.')
        else:
          _logger.debug('no checksum check will be performed, forcing file copy source %r to destination %r.', _local_path, _hdfs_path)


      if not skip:
        if osp.dirname(_tmp_path) not in dircache:
          _logger.info('Parent directory %r does not exist, creating recursively.', osp.dirname(_tmp_path))
          # Prevent race condition when creating directories
          with lock:
            if self.status(osp.dirname(_tmp_path), strict=False) is None:
              curpath = ''
              root_dir = None
              for dirname in osp.dirname(_tmp_path).strip('/').split('/'):
                curpath = '/'.join([curpath, dirname])
                if self.status(curpath, strict=False) is None:
                  if root_dir is not None:
                    root_dir = curpath
                  self.makedirs(curpath)
                  dircache.append(curpath)
                  if preserve:
                    curr_local_path=osp.realpath( osp.join( _local_path,osp.relpath(curpath,_tmp_path)) )
                    _preserve(curr_local_path,curpath)
                else:
                  dircache.append(curpath)
            else:
              dircache.append(osp.dirname(_tmp_path))

        _logger.info('Uploading %r to %r.', _local_path, _tmp_path)

        with open(_local_path, 'rb') as reader:
          self.write(_tmp_path, wrap(reader, chunk_size, progress), **kwargs)

        if _tmp_path != _hdfs_path:
          _logger.info( 'Upload of %r complete. Moving from %r to %r.', _local_path, _tmp_path, _hdfs_path )
          self.delete(_hdfs_path)
          self.rename(_tmp_path, _hdfs_path)
        else:
          _logger.info(
            'Upload of %r to %r complete.', _local_path, _hdfs_path
          )

        if preserve:
          _preserve(_local_path,_tmp_path)

        return { 'status': 'copied', 'src_path': _local_path, 'dest_path' : _hdfs_path }
      else:
        # file was skipped
        if progress:
          progress(_local_path, int(osp.getsize(_local_path)))
          progress(_local_path, -1)
        return { 'status': 'skipped', 'src_path': _local_path, 'dest_path' : _hdfs_path }


    # Normalise local and remote paths
    local_path = osp.normpath(local_path)
    hdfs_path = self.resolvepath( hdfs_path )

    # First, resolve the list of local files/directories to be uploaded
    uploads = [ upload_file for upload_file in glob.iglob(local_path) ]

    # need to develop a propper pattern based access function
    if len(uploads) == 0:
      raise HdfsError('Cloud not resolve source path, either it does not exist or can not access it.', local_path)

    tuples = []
    for upload in uploads:
      upload_tuple = dict()
      try:
        #filename = osp.basename(upload)
        #hdfs_base_path =  osp.join( hdfs_path, filename )
        status = self.status(hdfs_path,strict=True)
        #statuses = [status for _, status in self.list(hdfs_base_path)]
      except HdfsError as err:
        if 'File does not exist' in str(err):
          # Remote path doesn't exist.
          # check if parent exist
          try:
            pstatus = self.status(osp.dirname(hdfs_path),strict=True)
          except HdfsError, err:
            raise HdfsError('Parent directory of %r does not exist.', hdfs_path)
          else:
            # Remote path does not exist, and parent exist
            # so we want the source to be renamed as destination
            # so do not add the basename
            hdfs_base_path =  hdfs_path
            upload_tuple = dict({ 'local_path' : upload, 'hdfs_path' : hdfs_base_path})
      else:
        # Remote path exists.
        if status['type'] == 'FILE':
          # Remote path exists and is a normal file.
          if not overwrite:
            raise HdfsError('Remote path %r already exists.', hdfs_path)
          # the file is going to be deleted and the destination is going to be created with the same name
          hdfs_base_path = hdfs_path
        else:
          # Remote path exists and is a directory.
          if files_only == True:
             hdfs_base_path = hdfs_path
          else:
            try:
              status = self.status(osp.join( hdfs_path, osp.basename(upload) ),strict=True)
            except HdfsError as err:
              if 'File does not exist' in str(err):
                # destination does not exist, great !
                hdfs_base_path =  osp.join( hdfs_path, osp.basename(upload) )
                pass
            else:
              # destination exists
              hdfs_base_path = osp.join( hdfs_path, osp.basename(upload))
              if not overwrite:
                raise HdfsError('Remote path %r already exists.', hdfs_base_path)

        upload_tuple = dict({ 'local_path' : upload, 'hdfs_path' : hdfs_base_path})
      finally:
        tuples.append(upload_tuple)

    # This is a workaround for a Bug when uploading files using a pattern
    # it may happen that files can have the same name: ex :
    # /home/user/test/*/*.py may result in duplicate files
    for i in range(0, len(tuples)):
        for x in range(i + 1, len(tuples)):
          if tuples[i]['hdfs_path'] == tuples[x]['hdfs_path']:
            raise HdfsError('Conflicting files %r and %r : can\'t copy both files to %r'
                            % (tuples[i]['local_path'], tuples[x]['local_path'], tuples[i]['hdfs_path']) )

    fpath_tuples = []
    for upload_tuple in tuples:
      # Then we figure out which files we need to upload, and where.   
      if osp.isdir(upload_tuple['local_path']):
        local_fpaths = []
        for dpath, _, fpaths in os.walk(upload_tuple['local_path']):
          for fpath in fpaths:
            if fnmatch.fnmatch(fpath, include_pattern):
              if osp.getsize(osp.join(dpath, fpath)) >= int(min_size):
                local_fpaths.append(osp.join(dpath, fpath))

        if files_only == False:
          offset = len(upload_tuple['local_path'].rstrip(os.sep)) + len(os.sep)
          fpath_tuples.extend([
            (fpath, osp.join(upload_tuple['hdfs_path'], fpath[offset:].replace(os.sep, '/')))
            for fpath in local_fpaths
          ])
        else:
          fpath_tuples.extend([
            (fpath, osp.join(upload_tuple['hdfs_path'], osp.basename(fpath)))
            for fpath in local_fpaths
          ])
      elif osp.exists(upload_tuple['local_path']):
        fpath_tuples.append((upload_tuple['local_path'], upload_tuple['hdfs_path']))
      else:
        raise HdfsError('Local path %r does not exist.', upload_tuple['local_path'])

    _logger.info("--- scan finished in %s seconds, uploading %s files ---" % (time.time() - start_time, len(fpath_tuples)))

    if len(fpath_tuples) == 0:
      end_time = time.time()
      _logger.warn("could not find any file to upload")
      return {
        'Source Path'      : local_path,
        'Destination Path' : hdfs_path,
        'Start Time'       : datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
        'End Time'         : datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
        'Duration'         : end_time - start_time,
        'Outcome'          : 'Successful',
        'Files Expected'   : 0,
        'Files Copied'     : 0,
        'Files Failed'     : 0,
        'Files Deleted'    : 0,
        'Files Skipped'    : 0,
      }

    # Finally, we upload all files (optionally, in parallel).
    if n_threads <= 0:
      n_threads = len(fpath_tuples)
    else:
      n_threads = min(n_threads, len(fpath_tuples))
    _logger.debug(
      'Uploading %s files using %s thread(s).', len(fpath_tuples), n_threads
    )
    try:
      if n_threads == 1:
        results = []
        for path_tuple in fpath_tuples:
          results.append( _upload_wrap(path_tuple) )
      else:
        results = _map_async(n_threads, _upload_wrap, fpath_tuples)
    except Exception as err: # pylint: disable=broad-except
      _logger.exception('Error while uploading. Attempting cleanup.')
      raise err

    _logger.info("--- upload finished in : %s seconds ---" % (time.time() - start_time))

    end_time = time.time()

    # Transfer summary
    status = {
      'Source Path'      : local_path,
      'Destination Path' : hdfs_path,
      'Start Time'       : datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
      'End Time'         : datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
      'Duration'         : end_time - start_time,
      'Outcome'          : 'Successful',
      'Files Expected'   : 0,
      'Files Copied'     : 0,
      'Files Failed'     : 0,
      'Files Deleted'    : 0,
      'Files Skipped'    : 0,
    }

    for result in results:
      status['Files Expected']+=1
      if result['status'] == 'copied':
        status['Files Copied']+=1
      if result['status'] == 'skipped':
        status['Files Skipped']+=1
      if result['status'] == 'failed':
        status['Files Failed']+=1
        status['Outcome'] = 'Failed'

    return status

  # be careful the original resolve function provided with hdfs module does not seems to work
  # well with patterns, use this instead.
  def resolvepath(self, hdfs_path):
    """Return absolute, normalized path.
    :param hdfs_path: Remote path.
    """
    path = hdfs_path
    if not psp.isabs(path):
      if not self.root or not psp.isabs(self.root):
        root = self._api_request(method='GET', hdfs_path='/', params={'op': 'GETHOMEDIRECTORY'}).json()['Path']
        self.root = psp.join(root, self.root) if self.root else root
        _logger.debug('Updated root to %r.', self.root)
      path = psp.join(self.root, path)
    path = psp.normpath(path)
    _logger.debug('Resolved path %r to %r.', hdfs_path, path)
    return path


  def set_namequota(self, path, quota):
        if quota is None:
            return False
        if quota == "-1" :
            try:
                call("hdfs dfsadmin -clrQuota %s" % path, shell=True)
            except Exception, e:
                raise HdfsError(str(e))
        else:
            try:
                # I don't know why this seems to work only this way
                call("hdfs dfsadmin -setQuota %s %s" % (quota, path), shell=True)
            except Exception, e:
                raise HdfsError(str(e))
        return True

  def set_spacequota(self, path, quota):
        if quota is None:
            return False

        if quota == "-1" :
            try:
                call("hdfs dfsadmin -clrSpaceQuota %s" % path, shell=True)
            except Exception, e:
                raise HdfsError(str(e))
        else:
            try:
                call("hdfs dfsadmin -setSpaceQuota %s %s" % (quota, path), shell=True)
            except Exception, e:
                raise HdfsError(str(e))
        return True


class InsecureWebHDFSClient(WebHDFSClient):

  def __init__(self, nameservices, user=None, **kwargs):
    self.user = user or getuser()
    session = kwargs.setdefault('session', rq.Session())
    if not session.params:
      session.params = {}
    session.params['user.name'] = self.user
    super(InsecureWebHDFSClient, self).__init__(nameservices, **kwargs)

  def get_authenticated_user(self):
      return self.user

class KrbWebHDFSClient(WebHDFSClient):

  def __init__(self, nameservices, mutual_auth='DISABLED', **kwargs):
    session = kwargs.setdefault('session', rq.Session())
    if KRB_LIB_IMPORT == False:
        raise ImportError("Missing requests_kerberos library")
    if mutual_auth:
        try:
          _mutual_auth = getattr(requests_kerberos, mutual_auth)
        except AttributeError:
          raise HdfsError('Invalid mutual authentication type: %r', mutual_auth)
    else:
        _mutual_auth = mutual_auth
    session.auth = HTTPKerberosAuth(_mutual_auth)

    # get the authenticated user
    ctx = krbV.default_context()
    cc = ctx.default_ccache()
    try:
      self.principal = cc.principal().name
    except krbV.Krb5Error, e:
      raise HdfsError('Could not find any valid ticket in cache, %s', e)

    super(KrbWebHDFSClient, self).__init__(nameservices, **kwargs)

  def get_authenticated_user(self):
      # get the authenticated user
      return self.principal

class TokenWebHDFSClient(WebHDFSClient):

  def __init__(self, nameservices, token, **kwargs):
      session = kwargs.setdefault('session', rq.Session())
      if not session.params:
        session.params = {}
      session.params['delegation'] = token
      super(TokenWebHDFSClient, self).__init__(nameservices, **kwargs)

  def get_authenticated_user(self):
      # There is no way to fetch who is the authenticated user with token.
      return None

class KrbTokenWebHDFSClient(WebHDFSClient):

  def __init__(self, nameservices, mutual_auth='DISABLED', **kwargs):
      session = kwargs.setdefault('session', rq.Session())
      if not session.params:
        session.params = {}
      # try to create a renewer
      try:
        self.renewer = KrbWebHDFSClient( nameservices=nameservices, mutual_auth=mutual_auth, **kwargs)
      except HdfsError, e:
        _logger.error('Could not create a token renewer: %s',str(e))
        raise e
      # try to create a token
      try:
        _logger.info('Create delegation token, for user %s', self.renewer.get_authenticated_user())
        self.token = self.renewer.getDelegationToken(renewer=self.renewer.get_authenticated_user())['urlString']
      except HdfsError, e:
        _logger.error('Could not create a delegation token, %s', str(e))
        raise e
      session.params['delegation'] = self.token
      super(KrbTokenWebHDFSClient, self).__init__(nameservices, **kwargs)

  def __del__(self):
    try:
      _logger.info('Cleanup created delegation token.')
      self.renewer.cancelDelegationToken(token=self.token)
    except HdfsError, e:
      _logger.error('Could not cancel delegation token : %s', str(e))

  def _api_request(self, method, params, hdfs_path, data=None, strict=True, **rqargs):
    """Wrapper function."""
    max_attemps = self.host_list.get_host_count(hdfs_path)
    attempt = 0
    renew_attempted = False

    while True:
      host = self.host_list.get_active_host(hdfs_path)
      url = '%s%s%s' % (
        host.rstrip('/'),
        webhdfs_prefix,
        self.resolvepath(hdfs_path),
      )
      try:
        response = self._request(
          method=method,
          url=url,
          strict=strict,
          data=data,
          params=params,
          **rqargs
        )
        return response
      ## Handle stanby failover
      except StandbyError, e:
        _logger.warn('Namenode %s in standby mode. %s', host, str(e))
        self.host_list.switch_active_host(host,hdfs_path)
        attempt += 1
        if attempt >= max_attemps:
          raise HdfsError('Could not find any active namenode.')
        else:
          pass
      except InvalidTokenError, e:
        if renew_attempted == True:
          raise HdfsError('Could not renew Delegation Token ' % e)
        try:
          _logger.info('Renewing delegation token.')
          self.renewer.renewDelegationToken(token=self.token)
          renew_attempted = True
        except Exception, e2:
          raise HdfsError('Could not renew Delegation Token ' % e2)
    raise HdfsError('Inexpected Process End.')

    def get_authenticated_user(self):
      if self.created_token:
        return self.renewer.get_authenticated_user()
      else:
        # There is no way to fetch who is the authenticated user with token.
        return None

# Helpers
# -------

def _map_async(pool_size, func, args):
  """Async map (threading), handling python 2.6 edge case.

  :param pool_size: Maximum number of threads.
  :param func: Function to run.
  :param args: Iterable of arguments (one per thread).
  """
  pool = ThreadPool(pool_size)
  if sys.version_info <= (2, 6):
    return pool.map(func, args)
  else:
    return pool.map_async(func, args).get(1 << 31)
