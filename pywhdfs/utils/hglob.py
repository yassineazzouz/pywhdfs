"""HDFS globbing utility."""

import sys
import os
import re
import fnmatch
from .utils import HdfsError

try:
    _unicode = unicode
except NameError:
    # If Python is built without Unicode support, the unicode type
    # will not exist. Fake one.
    class _unicode(object):
        pass

__all__ = ["glob", "iglob"]

def glob(client, pathname):
    """Return a list of paths matching a pathname pattern.

    The pattern may contain simple shell-style wildcards a la
    fnmatch. However, unlike fnmatch, filenames starting with a
    dot are special cases that are not matched by '*' and '?'
    patterns.

    """
    return list(iglob(client,pathname))

def iglob(client, pathname):
    """Return an iterator which yields the paths matching a pathname pattern.

    The pattern may contain simple shell-style wildcards a la
    fnmatch. However, unlike fnmatch, filenames starting with a
    dot are special cases that are not matched by '*' and '?'
    patterns.

    """
    dirname, basename = os.path.split(pathname)
    if not has_magic(pathname):
        if basename:
            status = client.status(pathname,strict=False)
            if status is not None:
                yield pathname
        else:
            # Patterns ending with a slash should match only directories
            status = client.status(pathname,strict=False)
            if status is not None and status['type']=='DIRECTORY':
                yield pathname
        return
    if not dirname:
        for name in glob1(client, os.curdir, basename):
            yield name
        return
    # `os.path.split()` returns the argument itself as a dirname if it is a
    # drive or UNC path.  Prevent an infinite recursion if a drive or UNC path
    # contains magic characters (i.e. r'\\?\C:').
    if dirname != pathname and has_magic(dirname):
        dirs = iglob(client,dirname)
    else:
        dirs = [dirname]
    if has_magic(basename):
        glob_in_dir = glob1
    else:
        glob_in_dir = glob0
    for dirname in dirs:
        for name in glob_in_dir(client,dirname, basename):
            yield os.path.join(dirname, name)

# These 2 helper functions non-recursively glob inside a literal directory.
# They return a list of basenames. `glob1` accepts a pattern while `glob0`
# takes a literal basename (so it only has to check for its existence).

def glob1(client, dirname, pattern):
    if not dirname:
        dirname = os.curdir
    if isinstance(pattern, _unicode) and not isinstance(dirname, unicode):
        dirname = unicode(dirname, sys.getfilesystemencoding() or
                                   sys.getdefaultencoding())
    try:
        names = client.list(dirname)
    except HdfsError:
        return []
    if pattern[0] != '.':
        names = filter(lambda x: x[0] != '.', names)
    return fnmatch.filter(names, pattern)

def glob0(client, dirname, basename):
    if basename == '':
        # `os.path.split()` returns an empty basename for paths ending with a
        # directory separator.  'q*x/' should match only directories.
        status = client.status(dirname,strict=False)
        if status is not None and status['type']=='DIRECTORY':
            return [basename]
    else:
        status = client.status(os.path.join(dirname, basename),strict=False)
        if status is not None:
            return [basename]
    return []


magic_check = re.compile('[*?[]')

def has_magic(s):
    return magic_check.search(s) is not None
