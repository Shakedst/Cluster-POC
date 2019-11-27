import socket, pickle
import dill

def find_open_port(min_port, max_port):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    for port in range(min_port,max_port):
        try:
            #print 'Trying port:',port
            s.bind(('localhost',port))
            print 'Selected port:',port
            return port
        except socket.error as error:
            if error.errno == 10048:
                #print 'Port already in use'
                continue
            else:
                raise error
    return None

# Listening on TCP socket until server connects
wait_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
PORT = find_open_port(50000,51000)
#TODO: Bind to current IP instead of localhost, localhost only binds in loopback ip.
ADDR = ('localhost',PORT)
wait_sock.bind(ADDR)
#TODO: Find proper way to write to file AND DELETE FROM FILE
#with open('clients.txt','a+') as f:
#    f.write(pickle.dumps(ADDR) + '\r\n')
print 'Client waiting on', ADDR
wait_sock.listen(1)
s, server_addr = wait_sock.accept()
print 'Server connected from:', server_addr

#BUFFSIZE = 1024*1000 #TODO: Fix receiving of large data
BUFFSIZE = 10000000
# Receiving the function (Count in case of this POC) from the server
func_str = s.recv(BUFFSIZE)
func = dill.loads(func_str)
#print func_str
#print 'Testing func:',func('120253482')
s.send('Received function')
#Receiving the actual data we need to run
print 'Waiting for data'
str_data = s.recv(BUFFSIZE)
#print 'Data received. Length:', len(str_data)
print 'Data received'
data = pickle.loads(str_data)
ret = func(data)
print ret
str_ret = pickle.dumps(ret)
s.send(str_ret)
print 'Finished data sent to server'
s.close()
