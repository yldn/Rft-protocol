#########################################
#       RFT Client Implementation       #
#                                       #
#                                       #
#########################################

import rft_packet
import socket
import select
import datathread
import time
import os 
from pathlib import Path
from signal import signal, SIGINT
from sys import exit
from functools import partial



#Deal with the inital Handshake 
def exchange_handshake(socket_poll,client_socket,file_name,tries=3,timeout=10000):
    handshake_not_done = True
    first_packet = rft_packet.rft_packet.create_client_handshake(file_name)
    while(tries>0 and handshake_not_done):
        #Creating the first handshake packet
        client_socket.sendto(bytes(first_packet),server_addr)
    
        #Wait for response from the server (wait timeout for each iteration)
        event_list = socket_poll.poll(timeout)
        for fd, event in event_list:
            if(fd == client_socket.fileno()):
                data, addr = client_socket.recvfrom(1500)
                server_response = rft_packet.rft_packet(data)
                print(server_response)
                # assert serveraddr = receivedaddr 
                # handle status code response message
                if(server_response.isStc()):
                    if server_response.get_status_code == rft_packet.rft_status_codes.File_not_available :
                        print('file not found') 

                    return(False,None, None,None)
                    
                if(server_response.isNew()):
                    client_response = rft_packet.rft_packet.create_client_hello_ack(server_response.getCID(),dtr=20)
                    # set an initial datarate 
                    # print(client_response)
                    client_socket.sendto(bytes(client_response),server_addr)
                    handshake_not_done = False
                    #Handshake done 
                    print('Handshake "completed"')
                    return (True,client_response.getCID(),server_response.payload,client_response)

                

        tries -= 1
    
    return (False,None,None,None)

def compareBytes(b1,b2):
    list1  = str(list(b1))
    list2 = str(list(b2))
    return list1 == list2



def resumption_handshake(socket,socket_poll,checksum,filename,fileoffset,timeout=10000):
        # first resumption client hello  
    payload = checksum + str.encode(filename)
    client_resumption = rft_packet.rft_packet.create_resumption_packet(payload,0,fileoffset)
    # print('client res:',client_resumption)
        # receive resumption server hello 
    socket.sendto(bytes(client_resumption),server_addr)
    # print(rft_packet.rft_packet(bytes(client_resumption)))
        # send ack resumption server hello
    server_response = waitingforresponse(socket,socket_poll)
    # print(server_response)
    if(server_response is None or server_response.isStc()):
        return (False,None,None)

    print(server_response)
    if compareBytes(server_response.payload,checksum):
        client_resumption_ack = rft_packet.rft_packet.create_resumption_packet(b'',server_response.getCID(),0)
        client_resumption_ack.flags |= rft_packet.ACK
        # print(client_resumption_ack)
        socket.sendto(bytes(client_resumption_ack),server_addr)
        return True,server_response.getCID(),checksum

    return (False,None,None)

def waitingforresponse(connection_socket,socket_poll,timeout=10000): 
    event_list = socket_poll.poll(timeout)
    for fd, event in event_list:
        # if the file descriptor of the underlying socket equals to registered file descriptor.
        if(fd == connection_socket.fileno()):
            data, src = connection_socket.recvfrom(1500)
            packet = rft_packet.rft_packet(data)  
            return packet
    return None 


def transferResumption(socket,socket_poll,fileoffset,checksum):
    # print('here happens file transfer resumption')
    file_ok,cid,checksum = resumption_handshake(socket,socket_poll,checksum,file_name,fileoffset)

    signal(SIGINT,partial(grace_exithandler,socket,cid,checksum))

    if(file_ok):
        # delete the checksum
        f = open(file_name,"rb")
        content = f.read()
        content = content[:fileoffset]
        s = open(file_name,"wb")
        s.write(content)
        f.close()
        s.close()
        # time.sleep(10)
        print('resource free,checksum cleared')

        file= open(file_name,"ab")
        write_thread = datathread.data_write_queue(file,fileoffset)
        max_ack = 0
        packets_received = 0
        while(True):
            signal(SIGINT,partial(grace_exithandler,socket,cid,checksum))
            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if(fd == socket.fileno()):
                    res =  dataReception(socket,max_ack,cid,write_thread)
                    if res < 0 : 
                       return
                    max_ack = res
                 
                    packets_received += 1   

                    #TODO: after timeout ack
                    if(packets_received>29):
                        print("RES-ACK")
                        # packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = write_thread.get_missing_ranges())
                        packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = b'')
                        socket.sendto(bytes(packet),server_addr)
                        packets_received = 0
    
    else :
        print('something went wrong')
        return 
                

# send server stc ,organize packet ,capture signal and exit
def grace_exithandler(socket,CID,checksum,signal_received, frame):
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    # print('checksum:',checksum);
    termConnection(socket,CID,server_addr,checksum)

def termConnection(socket,CID,address,checksum):
    s = 'connection terminated!'
    print(s)
    termination_packet = rft_packet.rft_packet.create_status(CID,rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
    socket.sendto(bytes(termination_packet),address)

    if write_thread is not None:
        write_thread.stop()
        write_thread.set_fin()
        if not write_thread.run : 
    
        # gracefully termintaion will add checksum the unended file 
            append_data(file_name,checksum)
        #test
        # f= open(file_name,"r+b")
        # print('file content:',f.read())
        # print('file pos :',f.tell())
        # f.close()
    exit(0)


def append_data(file,data):
    f= open(file,"ab+")
    f.write(data)
    f.close()

def write_data_in_pos(file,data,pos):
    f= open(file,"r+b")
    content = f.read()
    content = content[:pos]+data+content[pos:]
    f.write(content)
    f.close()
    


#End point info

dst_addr = "127.0.0.1"
file_name  = "test.png"
port = 8888
home_port = 1111

write_thread = None

def connectionloop(client_socket,file):
    # global CID 
    # global cl_socket 
    global write_thread
    
    socket_poll = select.poll()
    socket_poll.register(client_socket, select.POLLIN)

    # Check if file exists
    if Path(file).is_file():
        print ("File {0} exists , Transfer resumption request...".format(file))
        # read current file offset & checksum
        f = open(file,"r+b")
        input_buffer= f.read()
        currentoffset = f.tell()
        currentoffset = currentoffset-32
        checksum = input_buffer[currentoffset:currentoffset+32]
        # print(currentoffset , checksum)

        transferResumption(client_socket,socket_poll,currentoffset,checksum)
        return

    #Handle handshake last_handshake_packet in case it is lost and needs to be resend
    file_ok, cid, checksum, last_handshake_packet  = exchange_handshake(socket_poll,client_socket,file)
    

    signal(SIGINT,partial(grace_exithandler,client_socket,cid,checksum))
    
    
    #Go into the transfer of the first file
    #TODO: send dtr, maybe use queue to determine the new data rate
    if(file_ok):
        
        file = open(file,"w+b")
        write_thread = datathread.data_write_queue(file,0)
        max_ack = 0
        packets_received = 0
        while(True):
            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if(fd == client_socket.fileno()):
                    res =  dataReception(client_socket,max_ack,cid,write_thread)
                    if res < 0 : 
                       return 

                    max_ack = res 
                    packets_received += 1   

                    #TODO: after timeout ack
                    if(packets_received>29):
                        print("ACK")
                        # packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = write_thread.get_missing_ranges())
                        packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = b'')
                        client_socket.sendto(bytes(packet),server_addr)
                        packets_received = 0
    else :
        print('something went wrong')
        return 


def dataReception(client_socket,max_ack,cid,datathread): 
        data, src = client_socket.recvfrom(1500)
        packet = rft_packet.rft_packet(data)
        # assert serveraddr = src 

        # print(packet)
        # status code check before write packet to buffer 
        # handle status code 
        if(packet.isStc()):
            # print(packet.get_status_code())
            if int.from_bytes(packet.get_status_code(),byteorder='big') == rft_packet.rft_status_codes.Connection_Terminated:
                print('server disconnected ')
                return -2

        datathread.add(packet)
        #update ACK
        max_ack = max(packet.getFileoffset()+len(packet.payload),max_ack)
        #TODO: Remove for testing only
        print(int.from_bytes(packet.file_offset,byteorder="big"))
        datathread.write()
        #check if packet is FIN 
        if(packet.isFin()):
            print("ACK_FIN")
            fin_packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.FIN)
            print(fin_packet)
            return -1

        return max_ack
                        
                    
            
def client(host,port,p,q,files):
    #Check for input address IPv4/IPv6
    IPv4 = True
    #for test 
    global server_addr 
    global file_name
    server_addr = (dst_addr,port)

    try:
        socket.inet_pton(socket.AF_INET,dst_addr)
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6,dst_addr)
        except socket.error:
            print("Invalid Address")
            return
        #Given address is IPv6
        IPv4  = False
    
    #Create socket for the given address type
    if(IPv4):
        client_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        client_socket.bind(("127.0.0.1",home_port))
    else:
        client_socket = socket.socket(socket.AF_INET6,socket.SOCK_DGRAM)
        client_socket.bind("::1",home_port)
    print('file request:',files)

    for i in range(len(files)):
        file_name=files[i]
        connectionloop(client_socket,files[i])

