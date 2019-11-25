import socket, threading, pickle, time, Queue
import dill #A library like pickle that can also serialize other objects types, like functions
from Count_digits import count

with open('nums.txt','r') as f:
    txt = f.read()

BUFFSIZE = 1024
func_str = dill.dumps(count)

lock = threading.Lock()
global arr
arr = [0]*10

def put_in_arr(data_arr):
    #TODO: Make server work with dictionary too
    global arr
    with lock:
        for i in range(len(arr)):
            arr[i] += data_arr[i]
    print arr

def get_clients():
    with open('clients.txt','r') as f:
        txt = f.read()
    pickle_arr = txt.split('\r\n')
    pickle_arr = filter(lambda x:x!='', pickle_arr)
    clients_arr = [pickle.loads(item) for item in pickle_arr]
    return clients_arr

clients = get_clients()
print clients

def handler(s,client_addr, func, data):
    thread_num = threading.current_thread().getName()
    s.connect(client_addr)
    print 'Socket %s connected to: %s' % (thread_num, client_addr)
    s.send(func)
    print 'Socket %s sent function' % thread_num
    time.sleep(1)
    data_str = pickle.dumps(data)
    s.send(data_str)
    print 'Socket %s sent data' % thread_num
    finished_data = s.recv(BUFFSIZE)
    data_arr = pickle.loads(finished_data)
    print data_arr
    put_in_arr(data_arr)
    

#Assisgning a socket for each client
sockets = dict([(socket.socket(socket.AF_INET, socket.SOCK_STREAM), client_addr) for client_addr in clients])
print sockets

N = len(clients)
part_size = len(txt)/N
parts = [txt[i*part_size:(i+1)*part_size] for i in range(N)]
parts[-1] = txt[(N-1)*part_size:]

threads = []
for s,addr in sockets.iteritems():
    index = len(threads)
    t = threading.Thread(target=handler, args=(s, addr, func_str, parts[index]))
    t.start()
    threads.append(t)
    
#global q
#q = Queue.Queue(0)
        
start = time.clock()

for t in threads:
    t.join()

'''
for i in range(len(threads)):
    arr2 = q.get(True)
    #TODO: Make server work with dictionary too
    print arr2
    for j in range(len(arr)):
        arr[j] += arr2[j]
'''

end = time.clock()
total_time = end-start
print total_time
print arr
print sum(arr)

