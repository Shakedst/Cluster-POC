import socket, threading, pickle, time, Queue
import dill #A library like pickle that can also serialize other objects types, like functions
#from Count_digits import count
#from count_words import count_words
import os # for reading folder

start = time.clock()

# Getting the function from the wanted file
with open(r'count_words.py','r') as f:
    # TODO after POC: Make this more generic, work if there's more than one function \ more things in file
    func_txt = f.read()
    exec(func_txt)
    print count_words  # count_words function is defined inside of the file, it exists now because we used exec
    #func_str = dill.dumps(count_words)

def read_folder(folder_name):
    total_text = ''
    os.chdir(folder_name)
    for file_name in os.listdir('.'):
        if os.path.isfile(file_name):
            with open(file_name,'r') as f:
                txt = f.read()
                total_text += ' ' + txt
    os.chdir('..')
    return total_text

txt = read_folder('texts folder')
print len(txt)

BUFFSIZE = 10000000
func_str = dill.dumps(count_words)

global lock
lock = threading.Lock()
global words
words = {}

def update_dict(data_dict,thread_num):
    #TODO: Make server work with dictionary too
    global words, lock
    print 'Thread', thread_num, 'waiting for lock. time:', time.clock()
    with lock:
        print 'Thread', thread_num, 'acquired lock. time:', time.clock()
        for word in data_dict.keys():
            if word in words.keys():
                words[word] += data_dict[word]
            else:
                words[word] = data_dict[word]
    max_word = max(words.keys(), key=lambda word:words[word])
    print 'Thread:', thread_num, 'Time:', time.clock(), 'max word:', max_word, data_dict[max_word], words[max_word]
    #print words

def get_clients():
    with open('clients.txt','r') as f:
        txt = f.readlines()
    #pickle_arr = txt.split('\r\n')
    #pickle_arr = filter(lambda x:x!='', pickle_arr)
    #clients_arr =[pickle.loads(item) for item in pickle_arr]
    print txt
    clients_arr = [eval(line) for line in txt]
    return clients_arr

clients = get_clients()
print 'Clients:',clients

def handler(s,client_addr, func, data, thread_num):
    #thread_num = threading.current_thread().getName()
    s.connect(client_addr)
    print 'Socket %s connected to: %s' % (thread_num, client_addr)
    s.send(func)
    #print 'Socket %s sent function' % thread_num
    confirmation = s.recv(BUFFSIZE)
    if confirmation != 'Received function':
        print confirmation
    data_str = pickle.dumps(data)
    s.send(data_str)
    print 'Socket %s sent data.' % thread_num
    returned_data_str = s.recv(BUFFSIZE)
    print 'Thread', thread_num, 'received data. time:', time.clock()
    returned_data = pickle.loads(returned_data_str)
    print 'Thread', thread_num, 'finished loading pickle. time:', time.clock()
    update_dict(returned_data, thread_num)

#Assisgning a socket for each client
sockets = dict([(socket.socket(socket.AF_INET, socket.SOCK_STREAM), client_addr) for client_addr in clients])
#print sockets

N = len(clients)
part_size = len(txt)/N
parts = [txt[i*part_size:(i+1)*part_size] for i in range(N)]
parts[-1] = txt[(N-1)*part_size:]

threads = []
for s,addr in sockets.iteritems():
    index = len(threads)
    t = threading.Thread(target=handler, args=(s, addr, func_str, parts[index], index))
    t.start()
    threads.append(t)
    
#global q
#q = Queue.Queue(0)

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
print 'Total time:',total_time
#print words
sorted_words = sorted(words.keys(), key=lambda word:words[word], reverse=True)
print 'Top 10 words:'
for i in range(10):
    print sorted_words[i], words[sorted_words[i]]

