#########################################
#       RFT Client Implementation       #
#                                       #
#                                       #
#########################################

import rft_packet
import socket
import select
import datathread
import hashlib
import time
import os 
from pathlib import Path
from signal import signal, SIGINT
from sys import exit
from functools import partial

def STChandler(packet):
    stc = packet.get_status_code()
    if stc == rft_packet.rft_status_codes.File_not_available :
        s = 'File Not Available received..connection terminated!'
        response = rft_packet.rft_packet.create_status(packet.getCID(),rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
        response.flags |= rft_packet.ACK
        return response
    if stc == rft_packet.rft_status_codes.File_changed :
        s = 'File changed / checksum mismatch...connection terminated!'
        response = rft_packet.rft_packet.create_status(packet.getCID(),rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
        response.flags |= rft_packet.ACK
        return response
    if stc == rft_packet.rft_status_codes.Connection_Terminated :
        pass
    else:
        pass
    
CID=0
#Deal with the inital Handshake 
def exchange_handshake(socket_poll,client_socket,file_name,tries=3,timeout=10000):
    global serverEndpoint,server_addr,server_port,CID
    handshake_not_done = True
    #Creating the first handshake packet
    print("creating the first handshake packet...of file: ",file_name)
    first_packet = rft_packet.rft_packet.create_client_handshake(file_name)
    while(tries>0 and handshake_not_done):
    ######################send to server ###############
        print(first_packet)
        client_socket.sendto(bytes(first_packet),serverEndpoint)

        #Wait for response from the server (wait timeout for each iteration)
        event_list = socket_poll.poll(timeout)
        for fd, event in event_list:
            if(fd == client_socket.fileno()):
                print('packet received...')
                data, addr = client_socket.recvfrom(1500)
                server_response = rft_packet.rft_packet(data)
                # print(server_response)
                # assert serveraddr = receivedaddr 
                # handle status code response message
                # statuscode acknoledgement
                if(server_response.isStc()):
                    print(server_response)
                    stcresponse = STChandler(server_response)
                    if stcresponse:
                        client_socket.sendto(bytes(stcresponse),serverEndpoint)
                    return (False,None, None,None)
                    
                if(server_response.isNew()):
                    CID=server_response.getCID()
                    client_response = rft_packet.rft_packet.create_client_hello_ack(CID,dtr=10)
                    # set an initial datarate 
                    # print(client_response)
    ######################send to server ###############                    
                    client_socket.sendto(bytes(client_response),serverEndpoint)

                    handshake_not_done = False
                    #Handshake done 
                    checksum = server_response.payload
                    print("CID:{0} , checksum:{1},".format(CID,checksum))
                    print('Handshake "completed"')
                    return (True,client_response.getCID(),checksum,client_response)

                

        tries -= 1
    
    return (False,None,None,None)

def compareBytes(b1,b2):
    list1  = str(list(b1))
    list2 = str(list(b2))
    return list1 == list2

def getfilechecksum(filename):
    if type(filename) != str :
        checksum = hashlib.sha256(open(filename.decode("utf-8"),"rb").read()).digest()
    else :
        checksum = hashlib.sha256(open(filename,"rb").read()).digest()

    return checksum


def resumption_handshake(socket,socket_poll,checksum,filename,fileoffset,timeout=10000):
        # first resumption client hello  
    payload = checksum + str.encode(filename)
    print("creating the resumption handshake packet...")
    client_resumption = rft_packet.rft_packet.create_resumption_packet(payload,0,fileoffset)
    # print('client res:',client_resumption)
        # send resumption server hello 
    ######################send to server ###############
    socket.sendto(bytes(client_resumption),serverEndpoint)

    # print(rft_packet.rft_packet(bytes(client_resumption)))
        # send ack resumption server hello
    server_response = waitingforresponse(socket,socket_poll)
    print('packet received...')
    # print(server_response)

    if(server_response is None ):
        return (False,None,None)

    if server_response.isStc() :
        # socket.sendto(bytes(server_response),serverEndpoint)
        print(server_response.payload)
        return (False,None,None)

    # print(server_response)
    if not compareBytes(server_response.payload,checksum):
        client_resumption_ack = rft_packet.rft_packet.create_resumption_packet(b'',server_response.getCID(),0)
        client_resumption_ack.flags |= rft_packet.ACK
        # print(client_resumption_ack)
    ######################send to server ###############
        socket.sendto(bytes(client_resumption_ack),serverEndpoint)
        print('Resumption Handshake "completed"')
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

    signal(SIGINT,partial(grace_exithandler,socket,cid))

    if(file_ok):

        file= open(file_name,"ab")
        write_thread = datathread.data_write_queue(file,fileoffset)
        dataTransfer(socket_poll,socket,cid,write_thread)
        return
    
    else :
        print('something went wrong')
        return 
                

# send server stc ,organize packet ,capture signal and exit
def grace_exithandler(socket,CID,signal_received, frame):
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    # print('checksum:',checksum);
    termConnection(socket,CID,serverEndpoint)

def termConnection(socket,CID,address):
    s = 'connection terminated!'
    print(s)
    termination_packet = rft_packet.rft_packet.create_status(CID,rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
    ######################send to server ###############  
    socket.sendto(bytes(termination_packet),address)

    exit(0)

def dataTransfer(socket_poll,socket,cid,write_thread):
    max_ack = 0
    packets_received = 0
    while(True):
        event_list = socket_poll.poll(0)
        for fd, event in event_list:
            if(fd == socket.fileno()):
                res =  dataReception(socket,max_ack,cid,write_thread)
                #-2 hard reset : -1 soft reset
                if res < -1 : 
                    return 
                if res < 0:
                    return

                max_ack = res 
                packets_received += 1   
                #TODO: after timeout ack 
                if(packets_received>9):
                    print("ACK")
                    # print(str(write_thread))
                    write_thread.write()
                    packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = write_thread.get_missing_ranges())
                    # print(packet)
                    # packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = b'')
######################send to server ###############  
                    socket.sendto(bytes(packet),serverEndpoint)
                    # print('ACK sent to server')
                    packets_received= 0

def dataReception(client_socket,max_ack,cid,datathread): 
        data, src = client_socket.recvfrom(1500)
        packet = rft_packet.rft_packet(data)
        # assert serveraddr = src 

        # print(packet)
        # status code check before write packet to buffer 
        # handle status code 
        if(packet.isStc()):
            # print(packet.get_status_code())
            stc = packet.get_status_code()
            if stc == rft_packet.rft_status_codes.Connection_Terminated or stc == rft_packet.rft_status_codes.Version_mismatch or stc ==rft_packet.rft_status_codes.Timeout or stc == rft_packet.rft_status_codes.Unknown:
                print('(Hard)server disconnected ')
                return -2 
            if stc == rft_packet.rft_status_codes.File_not_available or stc == rft_packet.rft_status_codes.File_changed:
                print('(Soft)server disconnected ')
                return -1
        
        #add oacket to receiving buffer
        datathread.add(packet)
        #update ACK
        max_ack = max(packet.getFileoffset()+len(packet.payload),max_ack)
        #TODO: Remove for testing only
        print(int.from_bytes(packet.file_offset,byteorder="big"))
        # datathread.write()
        #check if packet is FIN 
        if(packet.isFin()):
            print("ACK_FIN")
            fin_packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.FIN)
            # print(fin_packet)
            datathread.write()
            return -1

        return max_ack
         

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
serverEndpoint = None
server_addr = 0
home_addr = "127.0.0.1"
file_name  = "test.png"
server_port = 0
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
        print ("File {0} exists , Analyzing...".format(file))
        # read current file offset & checksum
        #TODO: handle resumption read
        f = open(file,"r+b")
        f.read()
        currentoffset = f.tell()
        checksum = getfilechecksum(file)
        # print(currentoffset , checksum)

        transferResumption(client_socket,socket_poll,currentoffset,checksum)
        return

    #Handle handshake last_handshake_packet in case it is lost and needs to be resend
    file_ok, cid, checksum, last_handshake_packet  = exchange_handshake(socket_poll,client_socket,file)
    

    signal(SIGINT,partial(grace_exithandler,client_socket,cid))
    
    
    #Go into the transfer of the first file
    #TODO: send dtr, maybe use queue to determine the new data rate
    if(file_ok):
        file = open(file,"w+b")
        write_thread = datathread.data_write_queue(file,0)
        dataTransfer(socket_poll,client_socket,cid,write_thread)
        return
                        
    else :
        print('something went wrong')
        return 


               
                    
            
def client(host,port,p,q,files):
    #Check for input address IPv4/IPv6
    IPv4 = True
    #for test 
    global serverEndpoint,file_name,server_addr,server_port

    serverEndpoint = (host,port)
    if serverEndpoint is not None:
        server_addr = serverEndpoint[0]
        server_port = serverEndpoint[1]
    

    try:
        socket.inet_pton(socket.AF_INET,server_addr)
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6,server_addr)
        except socket.error:
            print("Invalid Address")
            return
        #Given address is IPv6
        IPv4  = False
    
    #Create socket for the given address type
    if(IPv4):
        client_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        client_socket.bind((home_addr,home_port))
        # client_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    else:
        client_socket = socket.socket(socket.AF_INET6,socket.SOCK_DGRAM)
        client_socket.bind("host",home_port)
    print('file request:',files)

    for i in range(len(files)):
        file_name=files[i]
        print('start :',file_name)
        connectionloop(client_socket,files[i])

