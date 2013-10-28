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
import ConfigParser
import io
import pwd
from collections import defaultdict

import hesiod

CONFIG_FILE = '/etc/pyhesiodfs/config.ini'
ATTACHTAB_PATH='/.attachtab'
CONFIG_DEFAULT = """
[PyHesiodFS]
# Show a "README"-esque file in the filesystem
show_readme = false

# Filename (omit the leading slash)
readme_filename = README.txt

# This is a multi-line string.  Each subsequent line must be indented
# '%(mountpoint)s' will be replaced with the mountpoint of the filesystem
# '%(blank)s' will be replaced by whitespace
readme_contents = This is the pyhesiodfs FUSE autmounter.
 %(blank)s
 To access a Hesiod filsys, just access %(mountpoint)s/name.
 %(blank)s
 If you're using the Finder, try pressing Cmd+Shift+G and then
 entering %(mountpoint)s/name

show_attachtab = true
"""

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

new_fuse = hasattr(fuse, '__version__')

fuse.fuse_python_api = (0, 2)

if not hasattr(fuse, 'Stat'):
    fuse.Stat = object

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

    def toTuple(self):
        return (self.st_mode, self.st_ino, self.st_dev, self.st_nlink,
                self.st_uid, self.st_gid, self.st_size, self.st_atime,
                self.st_mtime, self.st_ctime)

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
        self.mounts = defaultdict(dict)
        
        # Dictionary of fake read-only file paths and their contents
        self.ro_files = {}

        # Cache deletions for half a second - should give `ln -nsf`
        # enough time to make a new symlink
        self.negcache = defaultdict(negcache)
    
    def _initializeConfig(self, config):
        try:
            self.show_attachtab = config.getboolean('PyHesiodFS', 'show_attachtab')
        except ValueError:
            syslog(LOG_WARNING, "Bad value for 'show_attachtab' in config file, assuming 'True'")
            self.show_attachtab = True

        try:
            self.show_readme = config.getboolean('PyHesiodFS', 'show_readme')
        except ValueError:
            syslog(LOG_WARNING, "Bad value for 'show_readme' in config file, assuming 'False'")
            self.show_readme = False

        mountpoint = self.fuse_args.mountpoint
        # The args should be parsed at this point.
        assert mountpoint is not None
        readme_filename = config.get('PyHesiodFS', 'readme_filename')
        if len(readme_filename) < 1 or '/' in readme_filename:
            syslog(LOG_WARNING, "Invalid value for 'readme_filename' in config file, disabling readme file")
            self.show_readme = False
        # Add the leading slash
        readme_path = '/' + readme_filename
        readme_contents = config.get('PyHesiodFS', 'readme_contents') % {'mountpoint': mountpoint,
                                                                         'blank': ''}
        # Add a newline if the "file" doesn't end in it
        if readme_contents[-1] != "\n":
            readme_contents += "\n"

        if self.show_attachtab:
            self.ro_files[ATTACHTAB_PATH] = self.getAttachtab

        if self.show_readme:
            self.ro_files[readme_path] = readme_contents

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
        elif path in self.ro_files:
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            st.st_size = len(self._get_file_contents(path))
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
        if new_fuse:
            return st
        else:
            return st.toTuple()

    def getCachedLockers(self):
        return self.mounts[self._uid()].keys()

    def getAttachtab(self):
        attachtab = defaultdict(list)
        rv = ''
        for uid in self.mounts:
            for locker in self.mounts[uid]:
                attachtab[locker].append(uid)
        for l in attachtab:
            people = [_pwnam(x) for x in attachtab[l]]
            if people:
                rv += "%-23s %-23s %-19s %s\n" % (l, '/mit/' + l,
                                                  "%s%s%s" % ('{' if len(people) > 1 else '',
                                                              ','.join(people),
                                                              '}' if len(people) > 1 else ''), 'nosuid')
        return rv

    def findLocker(self, name):
        """Lookup a locker in hesiod and return its path"""
        if name in self.mounts[self._uid()]:
            return self.mounts[self._uid()][name]
        else:
            try:
                filsys = hesiod.FilsysLookup(name)
            except IOError, e:
                if e.errno in (errno.ENOENT, errno.EMSGSIZE):
                    raise IOError(errno.ENOENT, os.strerror(errno.ENOENT))
                else:
                    raise IOError(errno.EIO, os.strerror(errno.EIO))
            # FIXME check if the first locker is valid
            if len(filsys.filsys) >= 1:
                pointers = filsys.filsys
                pointer = pointers[0]
                if pointer['type'] == 'AFS' or pointer['type'] == 'LOC':
                    self.mounts[self._uid()][name] = pointer['location']
                    syslog(LOG_INFO, "Mounting "+name+" on "+pointer['location'])
                    return pointer['location']
                elif pointer['type'] == 'ERR':
                    syslog(LOG_NOTICE, "ERR for locker %s: %s" % (name, pointer['message'], ))
                    return None
                else:
                    syslog(LOG_NOTICE, "Unknown locker type "+pointer['type']+" for locker "+name+" ("+repr(pointer)+" )")
                    return None
            else:
                syslog(LOG_WARNING, "Couldn't find filsys for "+name)
                return None

    def getdir(self, path):
        return [(i, 0) for i in (['.', '..'] + [x[1:] for x in self.ro_files.keys()] + self.getCachedLockers())]

    def readdir(self, path, offset):
        for (r, zero) in self.getdir(path):
            yield fuse.Direntry(r)
            
    def readlink(self, path):
        return self.findLocker(path[1:])

    def open(self, path, flags):
        if path not in self.ro_files:
            return -errno.ENOENT
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        if path not in self.ro_files:
            return -errno.ENOENT
        contents = self._get_file_contents(path)
        slen = len(contents)
        if offset < slen:
            if offset + size > slen:
                size = slen - offset
            buf = contents[offset:offset+size]
        else:
            buf = ''
        return buf

    def symlink(self, src, path):
        if path == '/' or path in self.ro_files:
            return -errno.EPERM
        elif '/' not in path[1:]:
            self.mounts[self._uid()][path[1:]] = src
            self.negcache[self._uid()].remove(path[1:])
        else:
            return -errno.EPERM
    
    def unlink(self, path):
        if path == '/' or path in self.ro_files:
            return -errno.EPERM
        elif '/' not in path[1:]:
            del self.mounts[self._uid()][path[1:]]
            self.negcache[self._uid()].add(path[1:])
        else:
            return -errno.EPERM

def main():
    config = ConfigParser.RawConfigParser()
    # Ensure a "PyHesiodFS" section exists in the config file
    config.readfp(io.BytesIO(CONFIG_DEFAULT))
    config.read(CONFIG_FILE)

    try:
        usage = Fuse.fusage
        server = PyHesiodFS(version="%prog " + fuse.__version__,
                            usage=usage,
                            dash_s_do='setsingle')
        server.parse(errex=1)
    except AttributeError:
        usage="""
pyHesiodFS [mountpath] [options]

"""
        if sys.argv[1] == '-f':
            sys.argv.pop(1)
        server = PyHesiodFS()

    server._initializeConfig(config)
    try:
        server.main()
    except fuse.FuseError as fe:
        print >>sys.stderr, "An error occurred while starting PyHesiodFS:"
        print >>sys.stderr, fe
        sys.exit(1)

if __name__ == '__main__':
    main()
