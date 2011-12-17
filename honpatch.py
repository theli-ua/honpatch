#!/usr/bin/env python3
# -*- coding: utf-8-*-
# AS-IS, Source Code under Public Domain
def cleanup_callback(a,b):
    exit(1)
    pass
import xml.etree.ElementTree as etree
from threading import Thread
from time import sleep
try:
    #3.x
    from urllib.request import Request
    from urllib.request import urlopen
    from urllib.parse   import urlencode
    from urllib.parse   import quote
    from urllib.parse   import urlparse
    from http.client    import HTTPConnection
    from queue          import Queue
except:
    #2.7
    from urllib2        import Request
    from urllib2        import urlopen
    from urllib         import urlencode
    from urllib         import quote
    from urlparse       import urlparse
    from httplib        import HTTPConnection
    from Queue          import Queue
from multiprocessing import Value
import io,struct
import zipfile,shutil

USER_AGENT = "S2 Games/Heroes of Newerth/2.0.29.1/lac/x86-biarch"

def unserialize(s):
    return _unserialize_var(s)[0]

def _unserialize_var(s):
    return (
        { 'i' : _unserialize_int
        , 'b' : _unserialize_bool
        , 'd' : _unserialize_double
        , 'n' : _unserialize_null
        , 's' : _unserialize_string
        , 'a' : _unserialize_array
        }[s[0].lower()](s[2:]))

def _unserialize_int(s):
    x = s.partition(';')
    return (int(x[0]), x[2])

def _unserialize_bool(s):
    x = s.partition(';')
    return (x[0] == '1', x[2])

def _unserialize_double(s):
    x = s.partition(';')
    return (float(x[0]), x[2])

def _unserialize_null(s):
    return (None, s)

def _unserialize_string(s):
    (l, _, s) = s.partition(':')
    return (s[1:int(l)+1], s[int(l)+3:])

def _unserialize_array(s):
    (l, _, s) = s.partition(':')
    a, k, s = {}, None, s[1:]

    for i in range(0, int(l) * 2):
        (v, s) = _unserialize_var(s)

        if k != None:
            a[k] = v
            k = None
        else:
            k = v
    return (a,s[1:])


class Manifest:
    def __init__(self,xmlstring=None,xmlpath=None,os=None,arch=None):
        if not xmlstring and not xmlpath:
            self.os = os
            self.arch = arch
            self.files = {}
            self.version = '0.0.0'
        else:
            if xmlstring:
                root = etree.fromstring(xmlstring)
            elif xmlpath:
                root = etree.parse(xmlpath).getroot()
            self.version = root.attrib['version']
            self.arch = root.attrib['arch']
            self.os = root.attrib['os']
            files = {}
            for e in root:
                if e.tag == 'file':
                    files[e.attrib['path']] = e.attrib
            self.files = files

class Changeset:
    def __init__(self,s,d):
        oldset = frozenset(s.files.keys())
        newset = frozenset(d.files.keys())
        self.dels = oldset - newset
        self.adds = newset - oldset
        self.changes = frozenset([x for x in list(oldset & newset) if \
                s.files[x]['version'] != d.files[x]['version'] or \
                s.files[x]['checksum']!= d.files[x]['checksum']])

def getVerInfo(os,arch,masterserver):
    details = urlencode({'version' : '0.0.0.0', 'os' : os ,'arch' : arch}).encode('utf8')
    url = Request('http://{0}/patcher/patcher.php'.format(masterserver),details)
    url.add_header("User-Agent",USER_AGENT)
    data = urlopen(url).read().decode("utf8", 'ignore') 
    d = unserialize(data)
    return d

def copyPerm(s,d):
    for item in os.listdir(d):
        src = os.path.join(s,item)
        dst = os.path.join(d,item)
        try:
            shutil.copymode(src,dst)
        except:
            print('Error copying mode from {0} to {1}'.format(src,dst))
        if os.path.isdir(src):
            copyPerm(src,dst)

def copyDir(s,d):
    for item in os.listdir(s):
        src = os.path.join(s,item)
        dst = os.path.join(d,item)
        if os.path.isdir(src):
            if os.path.exists(dst):
                copyDir(src,dst)
            else:
                shutil.copytree(src,dst)
        else:
            shutil.copy(src,dst)
def fetch_single(baseurl,baseurl2,version,path,fetchdir,retrycount):
    parsedurl = urlparse(baseurl)
    parsedurl2 = urlparse(baseurl2)
    conn = HTTPConnection(parsedurl.hostname,port=parsedurl.port)
    conn2 = HTTPConnection(parsedurl2.hostname,port=parsedurl2.port)

    res = fetch(conn,conn2,retrycount,baseurl,fetchdir,version,path):
    conn.close()
    conn2.close()
    return res


def fetch(conn,conn2,retrycount,url,tempdir,version,path):
    #url.add_header("User-Agent",USER_AGENT)
    if version.count('.') == 3 and version.endswith('.0'):
        version = version[:-2]

    url = '{0}/{1}/{2}.zip'.format(url,version,path)
    url = urlparse(url).path
    url = quote(url)

    path = os.path.join(tempdir,path)
    dpath = os.path.dirname(path)
    if not os.path.exists(dpath):
        os.makedirs(dpath)
    f=open('{0}.zip'.format(path),'wb')
    done = False
    connections = [conn,conn2]
    currconnindex = 0
    notfound = 0
    while not done and retrycount > 0:
        currconnindex += 1
        currconnindex %= 2
        c = connections[currconnindex]
        try:
            c.request("GET", url)
            r1 = c.getresponse()
            if r1.status == 200:
                f.write(r1.read())
                f.close()
                done = True
                break
            elif r1.status == 404 and notfound == 0:
                notfound = 1
                continue
            else:
                raise Exception('spam', 'eggs')
        except:
            print('\nError fetching {0}, {1} retries left\n'.format(url,retrycount-1))
            c.close()
            c.connect()            
        retrycount -= 1
    return done
        
class CoolZip (zipfile.ZipFile):
    #def __init__(self, *args, **kw):
        #zipfile.ZipFile.__init__(self, *args, **kw)
        #print('\n init {0}\n'.format(self.filename))
    def read_raw(self,name):
        # Only open a new file for instances where we were not
        # given a file object in the constructor
        if self._filePassed:
            zef_file = self.fp
        else:
            zef_file = io.open(self.filename, 'rb')
        if isinstance(name, zipfile.ZipInfo):
            # 'name' is already an info object
            zinfo = name
        else:
        # Get info object for name
            try:
                zinfo = self.getinfo(name)
            except KeyError:
                if not self._filePassed:
                    zef_file.close()
                    raise
        zef_file.seek(zinfo.header_offset, 0)
        # Skip the file header:
        fheader = zef_file.read(zipfile.sizeFileHeader)
        if fheader[0:4] != zipfile.stringFileHeader:
            raise BadZipFile("Bad magic number for file header")
        fheader = struct.unpack(zipfile.structFileHeader, fheader)
        fname = zef_file.read(fheader[zipfile._FH_FILENAME_LENGTH])
        if fheader[zipfile._FH_EXTRA_FIELD_LENGTH]:
            zef_file.read(fheader[zipfile._FH_EXTRA_FIELD_LENGTH])
        return (zinfo,zef_file.read(zinfo.compress_size))
    def add_raw(self,path,zinfo,data):
        zinfo.header_offset = self.fp.tell()    # Start of header data
        zinfo.filename = path
        self.fp.write(zinfo.FileHeader())
        self.fp.write(data)
        self.fp.flush()
        if zinfo.flag_bits & 0x08:
            # Write CRC and file sizes after the file data
            self.fp.write(struct.pack("<LLL", zinfo.CRC, zinfo.compress_size,
                  zinfo.file_size))
        self.filelist.append(zinfo)
        self.NameToInfo[zinfo.filename] = zinfo        


class FetchThread( Thread ):
    def __init__(self,conn,conn2,queue,retrycount,status):
        super(FetchThread, self).__init__()
        self.conn = conn
        self.conn2 = conn2
        self.queue = queue
        self.retrycount = retrycount
    def run(self):
        q = self.queue
        c = self.conn
        c2 = self.conn2
        while not q.empty():
            try:
                task = q.get(False)
            except:
                break
            try:
                if not fetch(c,c2,self.retrycount,*task):
                    raise Exception('spam', 'eggs')
            except:
                print('\nError fetching {0}\n'.format(task[-1]))
                status.value = -1
                break
            q.task_done()
        self.conn.close()
        self.conn2.close()
def main():
    import argparse
    parser = argparse.ArgumentParser(description='HoN Patcher')
    parser.add_argument("-t","--tmpdir", dest="tmpdir", help="directory used for temporary files, needs to have patch_size+hon_size space, defaults to OS default")
    parser.add_argument("-s","--hondir", dest="hondir",help="source HoN directory, if you do not set this you need to set --os and --arch to patch from 'empty' version")
    parser.add_argument('-d','--destdir',dest='destdir',help="destination directory, if is not set defaults to source")
    parser.add_argument("-v","--to-version", dest="destver",help="destination version, defaults to latest available")
    parser.add_argument("-l","--list",action="store_true", dest="list", default=False,help="list changes between versions")
    parser.add_argument("--nofetch",action="store_true",dest="nofetch",default=False,help="skip fetching files(if you have already fetched them somehow f.e.)")
    parser.add_argument("--noapply",action="store_true",dest="noapply",default=False,help="skip applying patch")
    parser.add_argument("--no-cleanup",action="store_true",dest="nocleanup",default=False,help="do not cleanup temporary directory")
    parser.add_argument("--no-perm-copy",action="store_true",dest="noperm",default=False,help="do not copy permissions from files in source directory")
    parser.add_argument("--os",dest="os",help="os to fetch files for, used if source directory is not set")
    parser.add_argument("--arch",dest="arch",help="arch to fetch files for, used if source directory is not set")
    parser.add_argument("--resume",dest="resume",help="resume downloading, makes sense with non-empty tmpdir",action="store_true",default=False)
    parser.add_argument("--fetchthreads",dest="fetchthreads",type=int,help="number of threads to use when fetching files",default=4)
    parser.add_argument("--retry-count",dest="retrycount",type=int,help="retry count for failed downloads",default=5)
    parser.add_argument("-m","--masterserver", dest="masterserver", help="masterserver to use",default='masterserver.hon.s2games.com')

    options = parser.parse_args()

    import signal,os

    signal.signal(signal.SIGINT, cleanup_callback)
    signal.signal(signal.SIGTERM, cleanup_callback)


    curManifest = None
    if options.hondir:
        mpath = os.path.join(options.hondir,'manifest.xml')
        if os.path.exists(mpath):
            curManifest = Manifest(xmlpath=mpath)
        else:
            print ('manifest.xml not found in {0}'.format(parser.hondir))
    else:
        print('Source directory not set this will be a LOOONG time')
        if not options.os or not options.arch or not options.destdir:
            print('You NEED to set arch,os and destination dir if source directory is not set')
        else:
            curManifest = Manifest(os=options.os,arch=options.arch)

    if not curManifest:
        exit(1)

    verinfo = getVerInfo(curManifest.os,curManifest.arch,options.masterserver)
    if options.destver:
        destver = options.destver
    else:
        destver = verinfo['version']

    destdir = options.destdir
    if not options.destdir:
        destdir = options.hondir

    if destver != verinfo['version']:
        print('Warning! {0} is not the latest version ({1})'.format(destver,verinfo['version']))

    if curManifest.version == destver:
        print('{0} == {1}, nothing to update'.format(curManifest.version,destver))
        exit(0)

    print('Doing upgrade from source dir "{0}" to destination "{1}",\n\t version {2} => {3}'.format(options.hondir,destdir,curManifest.version,destver))

    print('Fetching manifest.xml for {0}'.format(destver))

    tempdir = options.tmpdir

    if not tempdir:
        import tempfile
        tempdir = tempfile.mkdtemp()
    fetchdir = os.path.join(tempdir,'honpatch')
    cleanupdir = tempdir
    tempdir = os.path.join(tempdir,'hon')
    print('Using {0} as the temporary directory'.format(tempdir))
    baseurl = verinfo[0]['url'] + verinfo[0]['os'] + '/' + verinfo[0]['arch'] + '/'
    baseurl2 = verinfo[0]['url2'] + verinfo[0]['os'] + '/' + verinfo[0]['arch'] + '/'

    print('Using base url: {0}'.format(baseurl))
    print('Using base url2: {0}'.format(baseurl2))

    if not options.nofetch:
        #parsedurl = urlparse(baseurl)
        #parsedurl2 = urlparse(baseurl2)
        #conn = HTTPConnection(parsedurl.hostname,port=parsedurl.port)
        #conn2 = HTTPConnection(parsedurl2.hostname,port=parsedurl2.port)

        #if not fetch(conn,conn2,options.retrycount,baseurl,fetchdir,destver,'manifest.xml'):
            #print("Error fetching manifest.xml")
            #exit(1)
        #conn.close()
        #conn2.close()
        #def fetch_single(baseurl,baseurl2,version,path,fetchdir,retrycount):
        if not fetch_single(baseurl,baseurl2,destver,'manifest.xml',fetchdir,options.retrycount):
            print("Error fetching manifest.xml")
            exit(1)

    mz = CoolZip(os.path.join(fetchdir,'manifest.xml.zip'))
    newManifest = Manifest(xmlstring=mz.read('manifest.xml').decode("utf8", 'ignore'))
    mz.close()

    if curManifest.version == newManifest.version:
        print('{0} == {1}, nothing to update'.format(curManifest.version,newManifest.version))
        exit(0)

    changeSet = Changeset(curManifest,newManifest)
    import sys
    if options.list:
        print('Listing changes between versions:\n')
        print('Deleted files:')
        for i in sorted(changeSet.dels):
            print('\t{0}'.format(i))
        print('New files:')
        for i in sorted(changeSet.adds):
            print('\t{0}'.format(i))
        print('Modified files:')
        for i in sorted(changeSet.changes):
            print('\t{0}'.format(i))
    print('')

    prevlen = 1
    tofetch = changeSet.adds | changeSet.changes
    if not options.nofetch:
        print('Fetching files using {0} threads'.format(options.fetchthreads))
        queue = Queue()
        resume = options.resume
        for f in tofetch:
            v = newManifest.files[f]['version']
            path = os.path.join(fetchdir,f + '.zip')
            if not resume or not os.path.exists(path) or os.path.getsize(path) != int(newManifest.files[f]['zipsize']):
                queue.put((baseurl,fetchdir,v,f))

        parsedurl = urlparse(baseurl)
        parsedurl2 = urlparse(baseurl2)
        status = Value('b',0)  

        for _ in range(options.fetchthreads):
            conn = HTTPConnection(parsedurl.hostname,port=parsedurl.port)
            conn2 = HTTPConnection(parsedurl2.hostname,port=parsedurl2.port)
            FetchThread(conn,conn2,queue,options.retrycount,status).start()
        total = len(tofetch)
        filler = ' '*20
        while not queue.empty() and status.value == 0:
            sys.stdout.write('{0}\rDone {1}/{2}\r'.format(filler,total-queue.qsize(),total))
            sys.stdout.flush()
            sleep(1)
        if status.value != 0:
            print('\n\nCould not fetch all files, exitting')
            exit(1)        
        queue.join()
        print('\n Fetching done.')
    else:
        print('Not actually fetching anything')

    if not options.noapply:
        print("Temporary applying patch to {0}".format(tempdir))
        files = sorted(newManifest.files.keys())
        files.append('manifest.xml')
        tofetch |= frozenset(['manifest.xml'])
        total = len(files)
        current = 1
        prevlen = 1
        s2z = {}
        s2z_source = {}
        for f in files:
            sys.stdout.write('{1}\rProcessing [{2}/{3}]{0}\r'.format(f,' '*prevlen,current,total))
            sys.stdout.flush()
            prevlen = len(f) + 30
            current += 1

            path = f.split('.s2z/')
            if f in tofetch:
                z = CoolZip(os.path.join(fetchdir,f + '.zip'))
                if len(path) == 1:
                    try:
                        data = z.read(os.path.basename(f))
                    except:
                        data = z.read(os.path.basename(f.lower()))
                else:
                    try:
                        data = z.read_raw(os.path.basename(f))
                    except:
                        data = z.read_raw(os.path.basename(f.lower()))
                z.close()
            else:
                if len(path) == 1:
                    try:
                        _raw_file = open(os.path.join(options.hondir,f),'rb')
                        data = _raw_file.read()
                        _raw_file.close()
                    except:
                        print('Could not read {0}'.format(os.path.join(options.hondir,f)))
                        data = bytes()
                else:
                    if path[0] not in s2z_source:
                        s2z_source[path[0]] = \
                            CoolZip(os.path.join(options.hondir,path[0] + '.s2z'))
                    data = s2z_source[path[0]].read_raw(path[1])

            #now that we have data we need to insert it into proper place

            if len(path) == 1:
                path = os.path.join(tempdir,f)
                dpath = os.path.dirname(path)
                if not os.path.exists(dpath):
                    os.makedirs(dpath)
                _dest_file = open(path,'wb')
                _dest_file.write(data)
                _dest_file.close()
            else:
                if path[0] not in s2z:
                    dpath = os.path.dirname(os.path.join(tempdir,path[0]))
                    if not os.path.exists(dpath):
                        os.makedirs(dpath)
                    z = CoolZip(os.path.join(tempdir,path[0] + '.s2z'),'w',zipfile.ZIP_DEFLATED)
                    s2z[path[0]] = z
                s2z[path[0]].add_raw(path[1],*data)
        print('')
        print('Closing s2z archives')
        del s2z
        del s2z_source

    if options.hondir and not options.noperm and not options.noapply:
        print('Copying permissions from {0}'.format(options.hondir))
        copyPerm(options.hondir,tempdir)

    if not options.noapply:
        print('Copying new version to {0}'.format(destdir))
        copyDir(tempdir,destdir)

    if not options.nocleanup:
        if options.tmpdir:
            for i in os.listdir(options.tmpdir):
                item = os.path.join(options.tmpdir,i)
                if os.path.isdir(item):
                    shutil.rmtree(item)
                else:
                    os.unlink(item)
        else:
            shutil.rmtree(cleanupdir)
    print('done')
if __name__ == '__main__':
    main()
