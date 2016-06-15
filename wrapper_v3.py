import subprocess, multiprocessing, Queue
from multiprocessing import Process
import os

def worker(i):
    #worker function
    i = "--port=" + i
    print i    
    command = ["python", "dataserver.py"]
    command.append(i)
    print command
    parent_return = subprocess.call(command, shell=False )
    print parent_return
    os._exit(0)

#current_port = '51240'
port_numbers = ['51234','51235','51236','51237','51238','51239']

def fork_function(port_number):
  child_pid = os.fork()
  if child_pid == 0: 
     worker(port_number)
  else:
     #print "parent" + port_number
     print ""
  return


for i in range(len(port_numbers)):
      #print "---------------" + port_numbers[i] 
      #print port_numbers[i]
      fork_function(port_numbers[i])

print "execution complete"








