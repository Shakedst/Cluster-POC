import socket, threading, pickle, time, Queue
import dill #A library like pickle that can also serialize other objects types, like functions
#from Count_digits import count
#from count_words import count_words
import os  #for reading folder
import multiprocessing

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

def update_dict(data_dict,thread_num, words):
    #Words - the total dictionary, shared between processes
    #global words
    #words = manager_dict
    #print id(words), id(manager_dict)
    print 'Thread', thread_num, 'waiting for lock. time:', time.clock()
    #print 'Thread', thread_num, 'acquired lock. time:', time.clock()
    for word in data_dict.keys():
        #print 'something'
        if word in words.keys():
            words[word] += data_dict[word]
        else:
            words[word] = data_dict[word]
    max_word = max(words.keys(), key=lambda word:words[word])
    print 'Thread:', thread_num, 'Time:', time.clock(), 'max word:', max_word, data_dict[max_word], words[max_word]

def get_clients():
    with open('clients.txt','r') as f:
        txt = f.readlines()
    print txt
    clients_arr = [eval(line) for line in txt]
    return clients_arr

def handler(client_addr, func, data, thread_num, manager_dict):
    BUFFSIZE = 10000000
    #s = dill.loads(sock_str)
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect(client_addr)
    print 'Socket %s connected to: %s' % (thread_num, client_addr)
    s.send(func)
    confirmation = s.recv(BUFFSIZE)
    if confirmation != 'Received function':
        print confirmation
    data_str = pickle.dumps(data)
    s.send(str(len(data_str)).zfill(10) + data_str)
    print 'Socket %s sent data.' % thread_num
    returned_data_str = s.recv(BUFFSIZE)
    data_len = int(returned_data_str[:10])
    returned_data_str = returned_data_str[10:]
    print 'Data len', thread_num, ':', data_len
    while len(returned_data_str) < data_len:
        if thread_num == 1:
            print len(returned_data_str)
        data = s.recv(8192)
        returned_data_str += data
    print 'Thread', thread_num, 'received data. time:', time.clock()
    try:
        returned_data = pickle.loads(returned_data_str)
    except:
        print 'Error. data length:', len(returned_data_str), 'supposed to be:', data_len
    print 'Thread', thread_num, 'finished loading pickle. time:', time.clock()
    update_dict(returned_data, thread_num, manager_dict)
    #q.put((returned_data,thread_num))

def main():
    #multiprocessing.freeze_support()

    manager = multiprocessing.Manager()
    manager_words = manager.dict()

    start = time.clock()

    # Getting the function from the wanted file
    with open(r'count_words.py', 'r') as f:
        # TODO after POC: Make this more generic, work if there's more than one function \ more things in file
        func_txt = f.read()
        exec (func_txt)
        # print count_words  # count_words function is defined inside of the file, it exists now because we used exec
        # func_str = dill.dumps(count_words)

    txt = read_folder('texts folder')
    print len(txt)

    BUFFSIZE = 10000000
    func_str = dill.dumps(count_words)

    global q
    q = multiprocessing.Queue()

    global words
    words = {}

    clients = get_clients()
    print 'Clients:', clients

    # Assisgning a socket for each client
    sockets = dict([(socket.socket(socket.AF_INET, socket.SOCK_STREAM), client_addr) for client_addr in clients])

    N = len(clients)
    part_size = len(txt) / N
    parts = [txt[i * part_size:(i + 1) * part_size] for i in range(N)]
    parts[-1] = txt[(N - 1) * part_size:]

    threads = []
    for s, addr in sockets.iteritems():
        index = len(threads)
        #sock_str = dill.dumps(s)
        t = multiprocessing.Process(target=handler, args=(addr, func_str, parts[index], index, manager_words))
        t.start()
        threads.append(t)

    for t in threads:
         t.join()

    # while True:
    #     if q.qsize() < 8:
    #         continue
    #     try:
    #         data = q.get_nowait()
    #         print data
    #         update_dict(data[0],data[1])
    #     except:
    #         break

    end = time.clock()
    total_time = end - start
    print 'Total time:', total_time
    sorted_words = sorted(words.keys(), key=lambda word: words[word], reverse=True)
    print 'Top 10 words:'
    for i in range(10):
        print sorted_words[i], words[sorted_words[i]]

if __name__ == '__main__':
    multiprocessing.freeze_support()

    main()



