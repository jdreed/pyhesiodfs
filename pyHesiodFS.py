#!/usr/bin/python2

#    pyHesiodFS:
#    Copyright (c) 2013  Massachusetts Institute of Technology
#    Copyright (C) 2007  Quentin Smith <quentin@mit.edu>
#    "Hello World" pyFUSE example:
#    Copyright (C) 2006  Andrew Straw  <strawman@astraw.com>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import sys
if sys.hexversion < 0x020600f0:
    sys.exit("Python 2.6 or higher is required.")

import os, stat, errno, time
from syslog import *
import fuse
from fuse import Fuse
from ConfigParser import RawConfigParser
import pwd
from collections import defaultdict

import locker

LEGACY_ATTACHTAB_PATH='.attachtab.legacy'
ATTACHTAB_PATH='.attachtab'

class PyHesiodFSConfigParser(RawConfigParser):
    """
    A subclass of RawConfigParser that provides a single place to
    store defaults, and ensures a section exists, along with
    per-platform default values for the config file.  Also override
    getboolean to provide a method that deals with invalid values.
    """
    CONFIG_FILES = { 'darwin': '/Library/Preferences/PyHesiodFS.ini',
                     '_DEFAULT': '/etc/pyhesiodfs/config.ini',
                     }

    CONFIG_DEFAULTS = { 'show_readme': 'false',
                        'readme_filename': 'README.txt',
                        'readme_contents': """
This is the pyhesiodfs FUSE autmounter.
%(blank)s
To access a Hesiod filsys, just access %(mountpoint)s/name.
%(blank)s
If you're using the Finder, try pressing Cmd+Shift+G and then
entering %(mountpoint)s/name""",
                        'show_attachtab': 'true',
                        'syslog_unavail': 'true',
                        'syslog_unknown': 'true',
                        'syslog_success': 'false',
                        }

    def __init__(self):
        RawConfigParser.__init__(self, defaults=self.CONFIG_DEFAULTS)
        self.add_section('PyHesiodFS')
        if sys.platform in self.CONFIG_FILES:
            self.read(self.CONFIG_FILES[sys.platform])
        else:
            self.read(self.CONFIG_FILES['_DEFAULT'])

    def getboolean(self, section, option):
        try:
            return RawConfigParser.getboolean(self, section, option)
        except ValueError:
            rv = RawConfigParser.getboolean(self, 'DEFAULT', option)
            syslog(LOG_WARNING,
                   "Invalid boolean value for %s in config file; assuming %s" % (option, rv))
            return rv

# Helper functions

def _pwnam(uid):
    """
    Try to convert the supplied uid to a name using the passwd
    database.  Return the name or, if anything fails, just return the
    uid as a string.
    """
    try:
        return pwd.getpwuid(uid).pw_name
    except:
        return str(uid)

class attachtab(defaultdict):
    def __init__(self, fusefs):
        super(attachtab, self).__init__(dict)
        self.fusefs = fusefs

    def __str__(self):
        return "\n".join(["%s:%s" % (k,v) for k,v in self[self.fusefs._uid()].items()]) + "\n"

    def _legacyFormat(self):
        at = defaultdict(list)
        rv = "%-23s %-23s %-19s %s\n" % ("filesystem", "mountpoint", "user", "mode")
        rv += "%-23s %-23s %-19s %s\n" % ("----------", "----------", "----", "----")
        for uid in self:
            for locker in self[uid]:
                at[locker].append(_pwnam(uid))
        for e in at:
            if e:
                rv += "%-23s %-23s %-19s %s\n" % (e, os.path.join(self.fusefs.mountpoint, e),
                                                  attachtab._listPrint(at[e]), 'w,nosuid')
        return rv

    @staticmethod
    def _listPrint(l):
        return "%s%s%s" % ('{' if len(l) > 1 else '',
                           ','.join(l),
                           '}' if len(l) > 1 else '')


class negcache(dict):
    """
    A set-like object that automatically expunges entries after
    they're been there for a certain amount of time.
    
    This only supports add, remove, and __contains__
    """
    
    def __init__(self, cache_time=0.5):
        self.cache_time = cache_time
    
    def add(self, obj):
        self[obj] = time.time()
    
    def remove(self, obj):
        try:
            del self[obj]
        except KeyError:
            pass
    
    def __contains__(self, k):
        if super(negcache, self).__contains__(k):
            if self[k] + self.cache_time > time.time():
                return True
            else:
                del self[k]
        return False

# Use the "new" API
fuse.fuse_python_api = (0, 2)

class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class FakeFiles(dict):
    """A dict-style object that holds pathnames which behave as fake
    read-only files, and their contents.  Constraints on the keys and
    values are enforced by raising ValueError or TypeError.
    """
    def __init__(self, path='/'):
        super(FakeFiles, self).__init__()
        self.path = path

    def __setitem__(self, k, v):
        if type(k) is not str:
            raise TypeError('Filenames must be strings')
        if type(v) is not str and not callable(v):
            raise TypeError('File contents must be strings or callable')
        f = k.strip()
        if f in ['.', '..', ''] or '/' in f:
            raise ValueError("Invalid filename: '%s'" % (k,))
        super(FakeFiles, self).__setitem__(self.path + f,v)

    def filenames(self):
        return [x[len(self.path):] for x in self]

    def __getitem__(self, k):
        v = super(FakeFiles, self).__getitem__(k)
        return v() if callable(v) else v

class PyHesiodFS(Fuse):

    def __init__(self, *args, **kwargs):
        Fuse.__init__(self, *args, **kwargs)
        
        openlog('pyhesiodfs', 0, LOG_DAEMON)
        
        try:
            self.fuse_args.add("allow_other", True)
        except AttributeError:
            self.allow_other = 1

        if sys.platform == 'darwin':
            self.fuse_args.add("noappledouble", True)
            self.fuse_args.add("noapplexattr", True)
            self.fuse_args.add("volname", "MIT")
            self.fuse_args.add("fsname", "pyHesiodFS")
        self.mounts = attachtab(self)
        
        # Dictionary of fake read-only file paths and their contents
        self.ro_files = {}
        self.files = FakeFiles()

        self.syslog_unavail = True
        self.syslog_unknown = True
        self.syslog_success = False

        # Cache deletions for half a second - should give `ln -nsf`
        # enough time to make a new symlink
        self.negcache = defaultdict(negcache)
    
    def parse(self, *args, **kwargs):
        Fuse.parse(self, *args, **kwargs)
        self.mountpoint = self.fuse_args.mountpoint
        # Ensure that we know where we're mounted at this point
        assert self.mountpoint is not None

    def _initializeConfig(self, config):
        self.syslog_unavail = config.getboolean('PyHesiodFS', 'syslog_unavail')
        self.syslog_unknown = config.getboolean('PyHesiodFS', 'syslog_unknown')
        self.syslog_success = config.getboolean('PyHesiodFS', 'syslog_success')
        self.show_attachtab = config.getboolean('PyHesiodFS', 'show_attachtab')
        self.show_readme = config.getboolean('PyHesiodFS', 'show_readme')

        if self.show_readme:
            try:
                readme_contents = config.get('PyHesiodFS', 'readme_contents') + "\n"
                self.files[config.get('PyHesiodFS', 'readme_filename')] = readme_contents % {'mountpoint': self.mountpoint,
                                                                                             'blank': ''}
            except ValueError as e:
                syslog(LOG_WARNING,
                       "config file: bad value for 'readme_filename'")
            except KeyError as e:
                syslog(LOG_WARNING,
                       "config file: bad substitution key (%s) in 'readme_contents'" % (e.message,))

        if self.show_attachtab:
            self.files[LEGACY_ATTACHTAB_PATH] = self.mounts._legacyFormat
        self.files[ATTACHTAB_PATH] = self.mounts.__str__

    def _get_file_contents(self, path):
        assert path in self.ro_files
        contents = self.ro_files[path]
        assert callable(contents) or type(contents) is str
        return contents() if callable(contents) else contents

    def _uid(self):
        return fuse.FuseGetContext()['uid']
    
    def _gid(self):
        return fuse.FuseGetContext()['gid']
    
    def _pid(self):
        return fuse.FuseGetContext()['pid']
    
    def getattr(self, path):
        st = MyStat()
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_gid = self._gid()
            st.st_nlink = 2
        elif path in self.files:
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            st.st_size = len(self.files[path])
        elif path.startswith('/.'):
            # Avoid spurious Hesiod errors by not even bothering
            # to lookup things beginning with '.'
            return -errno.ENOENT
        elif '/' not in path[1:]:
            if path[1:] not in self.negcache[self._uid()] and self.findLocker(path[1:]):
                st.st_mode = stat.S_IFLNK | 0777
                st.st_uid = self._uid()
                st.st_nlink = 1
                st.st_size = len(self.findLocker(path[1:]))
            else:
                return -errno.ENOENT
        else:
            return -errno.ENOENT
        return st

    def getCachedLockers(self):
        return self.mounts[self._uid()].keys()

    def findLocker(self, name):
        """Lookup a locker in hesiod and return its path"""
        if name in self.mounts[self._uid()]:
            return self.mounts[self._uid()][name]
        else:
            try:
                lockers = locker.lookup(name)
            except locker.LockerNotFoundError as e:
                if self.syslog_unknown:
                    syslog(LOG_NOTICE, str(e))
                return None
            except locker.LockerUnavailableError as e:
                if self.syslog_unavail:
                    syslog(LOG_NOTICE, str(e))
                return None
            except locker.LockerError as e:
                syslog(LOG_WARNING, str(e))
                return None
            # FIXME check if the first locker is valid
            for l in lockers:
                if l.attachable():
                    self.mounts[self._uid()][name] = l.path
                    syslog(LOG_INFO, "Mounting "+name+" on "+l.path)
                    return l.path
            syslog(LOG_WARNING, "Lookup succeeded for %s but no lockers could be attached." % (name))
        return None

    def getdir(self, path):
        return [(i, 0) for i in (['.', '..'] + self.files.filenames() + self.getCachedLockers())]

    def readdir(self, path, offset):
        for (r, zero) in self.getdir(path):
            yield fuse.Direntry(r)
            
    def readlink(self, path):
        return self.findLocker(path[1:])

    def open(self, path, flags):
        if path not in self.files:
            return -errno.ENOENT
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        if path not in self.files:
            return -errno.ENOENT
        contents = self.files[path]
        slen = len(contents)
        if offset < slen:
            if offset + size > slen:
                size = slen - offset
            buf = contents[offset:offset+size]
        else:
            buf = ''
        return buf

    def symlink(self, src, path):
        if path == '/' or path in self.files:
            return -errno.EPERM
        elif '/' not in path[1:]:
            self.mounts[self._uid()][path[1:]] = src
            self.negcache[self._uid()].remove(path[1:])
        else:
            return -errno.EPERM
    
    def unlink(self, path):
        if path == '/' or path in self.files:
            return -errno.EPERM
        elif '/' not in path[1:]:
            del self.mounts[self._uid()][path[1:]]
            self.negcache[self._uid()].add(path[1:])
        else:
            return -errno.EPERM

def main():
    config = PyHesiodFSConfigParser()

    usage = Fuse.fusage
    server = PyHesiodFS(version="%prog " + fuse.__version__,
                        usage=usage,
                        dash_s_do='setsingle')
    server.parse(errex=1)

    server._initializeConfig(config)
    try:
        server.main()
    except fuse.FuseError as fe:
        print >>sys.stderr, "An error occurred while starting PyHesiodFS:"
        print >>sys.stderr, fe
        sys.exit(1)

if __name__ == '__main__':
    main()
