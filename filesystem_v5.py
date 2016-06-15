#!/usr/bin/env python
import logging
from collections import defaultdict
import errno
from errno import ENOENT,ECONNREFUSED 
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import time
from time import sleep
import datetime
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
import md5
#import time


count = 0
MAX_ATTEMPTS=2
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str



class FileNode:
    def __init__(self,name,isFile,path,url,ports,meta_port,Qr,Qw):
        self.name = name
        self.path = path
        self.url = url
	self.Qr=Qr
	self.Qw=Qw
	self.ports=ports
	self.meta_port=meta_port
        self.isFile = isFile # true if node is a file, false if is a directory.
        self.put_init("data","") # used if it is a file
        self.set_meta({})
        self.set_list_nodes({})# contains a tuple of <name:FileNode>  used only if it is a dir. 


    def put(self,key,value):
        key = self.path+"&&"+key
	print "key!!!"
	print key
	self.multiple_put(key,value)

    def put_init(self,key,value):
        key = self.path+"&&"+key
	count=0
	print "key!!!"
	print key
	count=0
	list_t=[]
	list_t=self.server_check_put(list_t,key)
	print 'after check'
	if len(list_t)<len(ports):
	    print 'put not complete',list_t
	    return

	for i in list_t:
	    rpc = xmlrpclib.Server(url+':'+i)
	    count=0
	    for attempt in range(MAX_ATTEMPTS):
		    try:
			#hash_val = self.checkSumCreate(value)
			rpc.put(Binary(key), Binary(pickle.dumps(value)), 6000)
		    except EnvironmentError as exc:
			if exc.errno == errno.ECONNREFUSED:
			    print 'no connection to port', i
			    count=count+1
			    if count==1:
				print "sleeping in put_init 10"			    
   			        sleep(1)
			        raise ECONNREFUSED
				return None
            if count==MAX_ATTEMPTS:
		    print 'connection not established to port',i
		    return
	

    def multiple_put(self,key,value):
	global MAX_ATTEMPTS
	count=0
	list_t=[]
	self.server_check_put(list_t,key)
	if len(list_t)<len(ports):
	    print 'put not complete',list_t
	    self.restore_meta()	    
	    return
	for i in list_t:
	    rpc = xmlrpclib.Server(url+':'+i)
	    count=0
	    for attempt in range(MAX_ATTEMPTS):
		    try:
			hash_val = self.checkSumCreate(value)
			print 'hash_val!!!!!!',hash_val,i
			rpc.put(Binary(key), Binary(pickle.dumps(hash_val)), 6000)
		    except EnvironmentError as exc:
			if exc.errno == errno.ECONNREFUSED:
			    print 'no connection to port', i
			    count=count+1
		            	
			    if count==1:
				print "sleeping in multiple put 10"			    				
				sleep(1)
			    raise ECONNREFUSED
		    
            if count==MAX_ATTEMPTS:
		    print 'connection not established to port',i
		    return
	

    def restore_meta(self):
	dict_t={}
	print 'restore func!!!'
	# node to be deleted
        node=(self.path.rsplit('/',1)[1])
        # parent node
	print self.path
        parent_node=('/'+self.path.rsplit('/',1)[0])+'&&'+'list_nodes'
	print parent_node,node
        # remove node from parents list
        rpc = xmlrpclib.Server(url+':'+meta_port)
        res = rpc.get(Binary(parent_node))
	print res

        if "value" in res:
            list_nodes=pickle.loads(res["value"].data)
	    print 'before!!',list_nodes
        else:
	    print 'None in list_nodes'
	    return None
        del list_nodes[node]
	print list_nodes
        rpc.put(Binary(parent_node), Binary(pickle.dumps(list_nodes)), 6000)
	return
		
    def server_check(self,list_t,key):
	count=0
	global MAX_ATTEMPTS
	for i in ports:
	    rpc = xmlrpclib.Server(url+':'+i)	
	    count=0
	    for attempt in range(MAX_ATTEMPTS):
		    try:
			ret=rpc.check_key(Binary(key))
			print 'ret',ret
		    except EnvironmentError as exc:
			if exc.errno == errno.ECONNREFUSED:
			    print 'no connection to port', i
			    if count==1:
				print "sleeping in server_check 10"			    
	   		        sleep(1)
			    count=count+1

			    
		    else:
			if ret==False:
			   self.server_dump(i,key)			    
			else:
			   break
		    print 'count',count
	   	    
	
	    if count<MAX_ATTEMPTS:
	    	list_t.append(i)
		print list_t
	
	return list_t
			     

    def server_check_put(self,list_t,key):
	count=0
	global MAX_ATTEMPTS
	for i in ports:
	    rpc = xmlrpclib.Server(url+':'+i)	
	    count=0
	    for attempt in range(MAX_ATTEMPTS):
		    try:
			ret=rpc.check_key(Binary(key))
			print 'ret',ret
		    except EnvironmentError as exc:
			if exc.errno == errno.ECONNREFUSED:
			    print 'no connection to port', i
			    if count==1:
				print "sleeping in server_check_put 10"			    
	   		        sleep(1)
			    count=count+1

			    
		    else:
			   break
	    print 'count',count
	   	    
	
	    if count<MAX_ATTEMPTS:
	    	list_t.append(i)
		print list_t
	
	return list_t



				
    def get(self,key):
        key = self.path+"&&"+key
	dict_t={}
	error_port=0
	return self.multiple_get(key)
	
    def checkSumCreate(self,value):
  	hash1 = md5.new()
	hash1.update(value) 
	k = value + '!@#' + repr(hash1.digest()) 
        return k

    #using delimiter !@#
    def checkSumCheck(self,value):
	print value
  	k = value.find('!@#')
  	if (k==-1):
    	    return 'false'
	else:
	    k1 = value[:k]
    	    k2 = value[k+3:]
	    hash1 = md5.new()
	    hash1.update(k1)
	    if (repr(hash1.digest()) == k2):
	        return 'true'   
	    else:
      		return 'false'
        

    def multiple_get(self,key):
	global MAX_ATTEMPTS
	count=0
	list_t=[]
	dict_t={}
	self.server_check(list_t,key)
	print 'Qr and Qw and len of list',Qr,Qw,len(list_t)
	if len(list_t)<int(Qr):
	    print 'get not complete',list_t
	    return ""

	for i in list_t:
            rpc = xmlrpclib.Server(url+':'+i)
	    count=0
            for attempt in range(MAX_ATTEMPTS):
		    try:
		        res=rpc.get(Binary(key))
			if "value" in res:
		   	    dict_t[i]=(pickle.loads(res["value"].data))
			    print dict_t[i]
			    print "dict_t====" 
			    print dict_t
			    break	
		    except EnvironmentError as exc:
			if exc.errno == errno.ECONNREFUSED:
			    print 'no connection to port', i
			    if count==1:
				print "sleeping in multiple get for 10"			    
			        sleep(1)
			    raise ECONNREFUSED
	if not dict_t=={}:
	    return self.error_check(dict_t,key)		
	else:
	    return None	


   
    def error_check(self,d,key):
        k = d.keys()
	count=0
	print 'in error check!!!'
	print len(k),k
	#for i in xrange(0, len(k)):
	#    hash_check = self.checkSumCheck(dict[i])
	#    print hash_check
	#    if hask_check==False:
	#	count=count+1	
	#if count==0:
	#    print d[k[0]].split('!@#',1)[0]		
	#    return d[k[0]].split('!@#',1)[0]		
	
   	for i in xrange(0, len(k)):
   	   if d[k[i]] != d[k[(i+1)%len(k)]] and d[k[i]] != d[k[(i+2)%len(k)]]:
   	  	self.error_correct(k[i],d[k[(i+1)%len(k)]],key)
		return d[k[(i+1)%len(k)]]
        print d[k[0]].split('!@#',1)[0]		
        return d[k[0]].rsplit('!@#',1)[0]

    def error_correct(self,error_port,correction_val,key):
	print 'in error correct!!!'
	print correction_val
 	#key = self.path+"&&"+key
        rpc = xmlrpclib.Server(url+':'+error_port)
        rpc.put(Binary(key), Binary(pickle.dumps(correction_val)), 6000)  
    
    def server_dump(self,error_port,key):
	print 'in server dump'
	err_port=[]
	res_port=[]
	err_port.append(error_port)
	res_port=list(set(ports)-set(err_port))
	for i in res_port:
	    rpc = xmlrpclib.Server(url+':'+i)	
	    count=0
	    try:
		ret=rpc.check_key(Binary(key))
		print 'ret',ret
	    except EnvironmentError as exc:
		if exc.errno == errno.ECONNREFUSED:
		    print 'no connection to port', i
		    count=count+1
	    else:
		if ret==True:
		   rpc.fill(error_port)			
		   break
	    print 'count',count
   	    
	

    def set_data(self,data_blob):
        self.put("data",data_blob)
        

    def set_meta(self,meta):
	key = self.path+"&&"+'meta'
        rpc = xmlrpclib.Server(url+':'+meta_port)
        rpc.put(Binary(key), Binary(pickle.dumps(meta)), 6000)

    def set_list_nodes(self,list_nodes):
	key = self.path+"&&"+'list_nodes'
        rpc = xmlrpclib.Server(url+':'+meta_port)
        rpc.put(Binary(key), Binary(pickle.dumps(list_nodes)), 6000)

    def get_data(self):
        return self.get("data")

    def get_meta(self):
	key = self.path+"&&"+'meta'
        rpc = xmlrpclib.Server(url+':'+meta_port)
        res = rpc.get(Binary(key))
        if "value" in res:
            return pickle.loads(res["value"].data)
        else:
            return None

    def get_list_nodes(self):
	key = self.path+"&&"+'list_nodes'
        rpc = xmlrpclib.Server(url+':'+meta_port)
        res = rpc.get(Binary(key))
        if "value" in res:
            return pickle.loads(res["value"].data)
        else:
            return None
        

    def list_nodes(self):
        return self.get_list_nodes().values()

    def add_node(self,newnode):
        list_nodes = self.get_list_nodes()
        list_nodes[newnode.name]=newnode
        self.set_list_nodes(list_nodes)

    def contains_node(self,name): # returns node object if it exists
        if (self.isFile==True):
            return None
        else:
            if name in self.get_list_nodes().keys():
                return self.get_list_nodes()[name]
            else:
                return None


class FS:
    def __init__(self,url,ports,meta_port,Qr,Qw):
        self.url = url
	self.ports=ports
	self.meta_port=meta_port
	self.Qr=Qr
	self.Qw=Qw
        self.root = FileNode('/',False,'/',url,ports,meta_port,Qr,Qw)
        now = time()
        self.fd = 0
        self.root.set_meta(dict(st_mode=(S_IFDIR | 0755), st_ctime=now,st_mtime=now,\
                                         st_atime=now, st_nlink=2))
    # returns the desired FileNode object
    def get_node_wrapper(self,path): # pathname of the file being probed.
        # Handle special case for root node
        if path == '/':
            return self.root
        PATH = path.split('/') # break pathname into a list of components
        name = PATH[-1]
        PATH[0]='/' # splitting of a '/' leading string yields "" in first slot.
        return self.get_node(self.root,PATH,name) 


    def get_node(self,parent,PATH,name):
        next_node = parent.contains_node(PATH[1])
        if (next_node == None or next_node.name == name):
            return next_node
        else:
            return self.get_node(next_node,PATH[1:],name)

    def get_parent_node(self,path):
        parent_path = "/"+("/".join(path.split('/')[1:-1]))
        parent_node = self.get_node_wrapper(parent_path)
        return parent_node

    def add_node(self,node,path):
        parent_path = "/"+("/".join(path.split('/')[1:-1]))
        parent_node = self.get_node_wrapper(parent_path)
        parent_node.add_node(node)
        if (not node.isFile):
            meta = parent_node.get_meta()
            meta['st_nlink']+=1
            parent_node.set_meta(meta)
        else:
            self.fd+=1
            return self.fd

    def add_dir(self,path,mode):
        # create a file node
        temp_node = FileNode(path.split('/')[-1],False,path,self.url,self.ports,self.meta_port,self.Qr,self.Qw)
        temp_node.set_meta(dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time()))
        # Add node to the FS
        self.add_node(temp_node,path)
  

    def add_file(self,path,mode):
        # create a file node
        temp_node = FileNode(path.split('/')[-1],True,path,self.url,self.ports,self.meta_port,self.Qr,self.Qw)
        temp_node.set_meta(dict(st_mode=(S_IFREG | mode), st_nlink=1,
        st_size=0, st_ctime=time(), st_mtime=time(),
        st_atime=time()))
        # Add node to the FS
        # before we do that, we have to manipulate the path string to point
        self.add_node(temp_node,path)
        self.fd+=1
        return self.fd

    def write_file(self,path,data=None, offset=0, fh=None):
        # file will already have been created before this call
        # get the corresponding file node
        filenode = self.get_node_wrapper(path)
        # if data == None, this is just a truncate request,using offset as 
        # truncation parameter equivalent to length
        node_data = filenode.get("data")
        node_meta = filenode.get_meta()
        if (data==None):
            node_data = node_data[:offset]
            node_meta['st_size'] = offset
        else:
            node_data = node_data[:offset]+data
            node_meta['st_size'] = len(node_data)
        filenode.put("data",node_data)
        filenode.set_meta(node_meta)
        

    def read_file(self,path,offset=0,size=None):
        # get file node
        filenode = self.get_node_wrapper(path)
        # if size==None, this is a readLink request
        if (size==None):
            return filenode.get_data()
        else:
            # return requested portion data
            return filenode.get("data")[offset:offset + size]

    def rename_node(self,old,new):
        # first check if parent exists i.e. destination path is valid
        future_parent_node = self.get_parent_node(new)
        if (future_parent_node == None):
            raise  FuseOSError(ENOENT)
            return
        # get old filenodeobject and its parent filenode object
        filenode = self.get_node_wrapper(old)
        parent_filenode = self.get_parent_node(old)
        # remove node from parent
        list_nodes = parent_filenode.get_list_nodes()
        del list_nodes[filenode.name]
        parent_filenode.set_list_nodes(list_nodes)
        # if filenode is a directory decrement 'st_link' of parent
        if (not filenode.isFile):
            parent_meta = parent_filenode.get("meta")
            parent_meta["st_nlink"]-=1
            parent_filenode.set_meta(parent_meta)
        # add filenode to new parent, also change the name
        filenode.name = new.split('/')[-1]
        future_parent_node.add_node(filenode)

    def utimens(self,path,times):
        filenode = self.get_node_wrapper(path)
        now = time()
        atime, mtime = times if times else (now, now)
        meta = filenode.get_meta()
        meta['st_atime'] = atime
        meta['st_mtime'] = mtime
        filenode.set_meta(meta)


    def delete_node(self,path):
        # get parent node
        parent_filenode = self.get_parent_node(path)
        # get node to be deleted
        filenode = self.get_node_wrapper(path)
        # remove node from parents list
        list_nodes = parent_filenode.get_list_nodes()
        del list_nodes[filenode.name]
        parent_filenode.set_list_nodes(list_nodes)
        # if its a dir reduce 'st_nlink' in parent
        if (not filenode.isFile):
            parents_meta = parent_filenode.get_meta()
            parents_meta["st_nlink"]-=1
            parent_filenode.set_meta(parents_meta)

    def link_nodes(self,target,source):
        # create a new target node.
        temp_node = FileNode(target.split('/')[-1],True,target,self.url,self.ports,self.meta_port,self.Qr,self.Qw)
        temp_node.set_meta(dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source)))
        temp_node.set_data(source)
        # add the new node to FS
        self.add_node(temp_node,target)

    def update_meta(self,path,mode=None,uid=None,gid=None):
        # get the desired filenode.
        filenode = self.get_node_wrapper(path)
        # if chmod request
        meta = filenode.get_meta()
        if (uid==None):
            meta["st_mode"] &= 0770000
            meta["st_mode"] |= mode
        else: # a chown request
            meta['st_uid'] = uid
            meta['st_gid'] = gid
        filenode.set_meta(meta)

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self,url,ports,meta_port,Qr,Qw):
        global count # count is a global variable, can be used inside any function.
        count +=1 # increment count for very method call, to track count of calls made.
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time())) # print the parameters passed to the method as input.(used for debugging)
        print('In function __init__()') #print name of the method called

        self.FS = FS(url,ports,meta_port,Qr,Qw)
       
        
       
        
    def getattr(self, path, fh=None):
        global count
        count +=1
        print ("CallCount {} " " Time {} arguments:{} {} {}".format(count,datetime.datetime.now().time(),type(self),path,type(fh)))
        print('In function getattr()')
        
        file_node =  self.FS.get_node_wrapper(path)
        if (file_node == None):
            raise FuseOSError(ENOENT)
        else:
            return file_node.get_meta()


    def readdir(self, path, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function readdir()')

        file_node =  self.FS.get_node_wrapper(path)
        m = ['.','..']+[x.name for x in file_node.list_nodes()]
        print m
        return m

    def mkdir(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {}" "," "argumnets:" " " "path;{}" "," "mode:{}".format(count,datetime.datetime.now().time(),path,mode))
        print('In function mkdir()')       
        # create a file node
        self.FS.add_dir(path,mode)

    def create(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {} path {} mode {}".format(count,datetime.datetime.now().time(),path,mode))
        print('In function create()')
        
        return self.FS.add_file(path,mode) # returns incremented fd.

    def write(self, path, data, offset, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print ("Path:{}" " " "data:{}" " " "offset:{}" " "  "filehandle{}".format(path,data,offset,fh))
        print('In function write()')
        
        self.FS.write_file(path, data, offset, fh)
        return len(data)

    def open(self, path, flags):
        global count
        count +=1
        print ("CallCount {} " " Time {}" " " "argumnets:" " " "path:{}" "," "flags:{}".format(count,datetime.datetime.now().time(),path,flags))
        print('In function open()')

        self.FS.fd += 1
        return  self.FS.fd 

    def read(self, path, size, offset, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}" " " "arguments:" " " "path:{}" "," "size:{}" "," "offset:{}" "," "fh:{}".format(count,datetime.datetime.now().time(),path,size,offset,fh))
        print('In function read()')

        return self.FS.read_file(path,offset,size)

    def rename(self, old, new):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function rename()')

        self.FS.rename_node(old,new)

    def utimens(self, path, times=None):
        global count
        count +=1
        print ("CallCount {} " " Time {} Path {}".format(count,datetime.datetime.now().time(),path))
        print('In function utimens()')

        self.FS.utimens(path,times)

    def rmdir(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function rmdir()')

        self.FS.delete_node(path)

    def unlink(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function unlink()')

        self.FS.delete_node(path)

    def symlink(self, target, source):
        global count
        count +=1
        print ("CallCount {} " " Time {}" "," "Target:{}" "," "Source:{}".format(count,datetime.datetime.now().time(),target,source))
        print('In function symlink()')

        self.FS.link_nodes(target,source)

    def readlink(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function readlink()')
        
        return self.FS.read_file(path)

    def truncate(self, path, length, fh=None):
        global count
        print ("CallCount {} " " Time {}""," "arguments:" "path:{}" "," "length:{}" "," "fh:{}".format(count,datetime.datetime.now().time(),path,length,fh))
        print('In function truncate()')
        
        self.FS.write_file(path,offset=length)

    def chmod(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function chmod()')

        self.FS.update_meta(path,mode=mode)
        return 0

    def chown(self, path, uid, gid):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function chown()')

        self.FS.update_meta(path,uid=uid,gid=gid)
        
        
    
        
    
        

   
if __name__ == "__main__":
  if len(argv) != 3:
    print 'usage: %s <mountpoint> <remote hashtable>' % argv[0]
    #exit(1)
  url = argv[2]
  Qr=argv[3]
  Qw=argv[4]
  meta_port=argv[5]
  ports= argv[6:]	
  print url
  print ports
  print meta_port
  # Create a new HtProxy object using the URL specified at the command-line
  fuse = FUSE(Memory(url,ports,meta_port,Qr,Qw), argv[1], foreground=True)