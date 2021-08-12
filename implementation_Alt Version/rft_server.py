#########################################
#       RFT Server Implementation       #
#                                       #
#                                       #
#########################################


import collections
from rft_client import client
import rft_packet
import rft_congestion_control
import socket
import select
import hashlib
import datathread
import time
import rft_flow_control
import datetime
import random

from pathlib import Path
import os.path
from signal import signal, SIGINT
from sys import exit
from functools import partial


data_rate = 0
# client adress
client_Endpoint = None

#Other acks are ignored since the status code have a higher priority (reset connections)
#TODO: ONlY wait for connection terminated acks
def send_status(dst,cid,sock,type,msg,tries=3,timeout=10):
    global client_Endpoint
    status_packet = rft_packet.rft_packet.create_status(cid,type,rft_packet.STC,msg)
    
    ack_received = False
    socket_poll = select.poll()
    socket_poll.register(sock, select.POLLIN)
    while(tries>=0 and not ack_received):
        # print(status_packet)
        sock.sendto(bytes(status_packet),dst)

        #waiting for client_response
        event_list = socket_poll.poll(timeout)
        if(not event_list):
            tries -= 1
            sock.sendto(bytes(status_packet),dst)
            
        for fd, event in event_list:
            if(fd == sock.fileno()):
                data, src = sock.recvfrom(1500)
                client_Endpoint = src
                packet = rft_packet.rft_packet(data)
                #check if it is the ack for the stc
                if( packet.isAck()):
                    print(packet)
                    ack_received = True
                    break
                # if(not packet.isAck() and not packet.isStc()):
                #     continue
                

    return ack_received
    pass

CIDs = list()
def addConnectionID():
    newCID = random.getrandbits(32)
    while newCID in CIDs:
        newCID = random.getrandbits(32)

    CIDs.append(newCID)
    print('newconnectionID genetated: ', newCID)
    return newCID



def complete_handshake(packet,connection_socket,client_addr,tries=3,timeout=10):
    global data_rate 
    #Receive handshake message from client assign client addr 
    if(packet.getlength()>0):
        print(packet)

        file_name = packet.payload

        # handle invalid NEW packet from client 
        if(not packet.isNew() or packet.getCID() != 0 or packet.getFileoffset() != 0):
            # print(packet.flags,packet.cid,packet.getFileoffset())
            print("Invalid handshake packet")
            send_status(client_addr,0,connection_socket,rft_packet.rft_status_codes.Unknown,"New not set or CID or Fileoffset not 0")
            return (None, False, None, None,None)

        #Everything good so far 
        # check if DTR is set 
        if(packet.isDtr()):
            data_rate=packet.dtr

        if not checkfile(file_name) :
            return (None, False, None, None,None)
         
        # file = open(file_name.decode("utf-8"),"rb")

        checksum = hashlib.sha256(open(file_name.decode("utf-8"),"rb").read()).digest()

        #Consturct the Server Hello to send back to the client
        CID = addConnectionID()
        server_packet = rft_packet.rft_packet.create_server_handshake(CID,checksum) 
        # print('calculate checksum of request file : {0} now sending server hello to client : {1}'.format(checksum,client_addr[0]) )
        #Send the packet to the client and wait for a response
    ######################send to client ###############  
        connection_socket.sendto(bytes(server_packet),client_addr)

        #Wait for answer or resend
        socket_poll = select.poll()
        socket_poll.register(connection_socket, select.POLLIN)
        handshake_done = False
        
        while(handshake_done==False and tries>0):
            # Polls the set of registered file descriptors, and returns a possibly-empty list containing (fd, event) 
            event_list = socket_poll.poll(timeout)
            #new connection try = 3 
            if(not event_list):
                tries -= 1
                connection_socket.sendto(bytes(server_packet),client_addr)

            for fd, event in event_list:
                # if the file descriptor of the underlying socket equals to registered file descriptor.
                if(fd == connection_socket.fileno()):
                    # handle the client handshake response end 
                    data, src = connection_socket.recvfrom(1500)
                    #print(rft_packet.rft_packet(data))

                    packet = rft_packet.rft_packet(data)
                    client_addr = src
                    # check the vallidity of client respond
                    if(packet.isStc()):
                        #Any Status code in the handshake is a problem 
                        code = packet.get_status_code()
                        msg = packet.get_status_msg()+ "ACK!"
                        status_ack = rft_packet.rft_packet.create_status(CID,code,rft_packet.STC|rft_packet.ACK)
                        connection_socket.sendto(bytes(status_ack),src)
                        print("Status code {0} received in handshake phase , Invalid {1}".format(code,packet.get_status_msg()))
                        return (None, False, None, None)

                    if(not packet.isNew() and not packet.isAck() or not packet.getCID() in CIDs or packet.getFileoffset() != 0): 
                        print(packet.flags,packet.cid,packet.getFileoffset())
                        print("Invalid handshake packet")
                        send_status(src,connection_socket,rft_packet.rft_status_codes.Unknown,"New or ACK not set or CID is not cid or Fileoffset not 0")
                        return (None, False, None, None)


                    if(packet.isDtr()):
                        data_rate=packet.dtr
                        # print(data_rate)
                        # print(packet)

                    handshake_done = True
                    break
                    #Ok, everything is correct 
                    
    print('Handshake "completed"')
    return (file_name, handshake_done, CID,client_addr,socket_poll)




def timestamp():
    return datetime.datetime.now().timestamp()


def openfile(filename): 
    #try to open the file 
    try:
        fd = open(filename,"rb")
        return fd
    #handle file not avaliable
    except FileNotFoundError:
    #File not found send back status to terminate connection
        # send_status(client_Endpoint,0,connection_socket,rft_packet.rft_status_codes.File_not_available,"File not found")
        # print("File not found:",filename)
        return  None

def checkfile(filename):
    # print(filename.decode("utf-8"))
    return os.path.isfile(filename)
    

def waitingforresponse(connection_socket,socket_poll,timeout=10000): 
    event_list = socket_poll.poll(timeout)
    for fd in event_list:
        # if the file descriptor of the underlying socket equals to registered file descriptor.
        if( fd[1]&select.POLLIN):
            data, src = connection_socket.recvfrom(1500)
            packet = rft_packet.rft_packet(data)  
            return packet
    return None


def resumption_handshake(packet,socket_poll,connection_socket,client_addr,tries=3,timeout=10000):
        # print(packet)
        checksum = packet.payload[:32]
        file_name = packet.payload[32:]

        if not checkfile(file_name) :
            send_status(client_Endpoint,0,connection_socket,rft_packet.rft_status_codes.File_not_available,"File not available")
            None,False,None, None


        file = open(file_name.decode("utf-8"),"rb")
        checksumcheck = hashlib.sha256(file.read()).digest()
        if checksumcheck == checksum:
            #send status code 
            send_status(client_Endpoint,0,connection_socket,rft_packet.rft_status_codes.Connection_Terminated,"File complete nothing to transfer")
            return None,False,None, None

        CID = addConnectionID()
        FO = packet.getFileoffset()
        server_response = rft_packet.rft_packet.create_resumption_packet(checksumcheck,CID,FO)
        # print('send:',server_response)
        connection_socket.sendto(bytes(server_response),client_addr)

        # response = waitingforresponse(socket,socket_poll)
        handshake_done=False
        
        packet = waitingforresponse(connection_socket,socket_poll)
        if(packet is None or packet.isStc()):
            return None,False,None, None
        
        if(packet.isRes()&packet.isAck()):
            handshake_done = True
            print('Resumption Handshake "completed"')
            return file_name,handshake_done,CID,FO
        else : 
            return None,False,None, None
 
def connection_loop(connection_socket):
    global client_Endpoint
    # analyzing incoming data 
    
    socket_poll = select.poll()
    socket_poll.register(connection_socket, select.POLLIN)

    data, addr = connection_socket.recvfrom(1500)
    client_Endpoint = addr
    print('packet received from: ',client_Endpoint)
    packet = rft_packet.rft_packet(data)
    print(packet)

    if packet.isRes():
    #is a resume handshake packet!
            # print(packet)
            filename,file_ok,cid,fileoffset = resumption_handshake(packet,socket_poll,connection_socket,client_Endpoint)
            signal(SIGINT,partial(grace_exithandler,connection_socket,cid))
            print(filename)
            print(file_ok)
            print(cid)
            print(fileoffset)
            if(file_ok):
                dataTransfer(connection_socket,socket_poll,cid,100,filename,fileoffset,1000)
            else:
                print('something of file went wrong')


    if packet.isNew(): 
        #is a new handshake packet! 
        filename, valid, cid, src_addr, socket_poll = complete_handshake(packet,connection_socket,client_Endpoint)

        signal(SIGINT,partial(grace_exithandler,connection_socket,cid))
        #Handshake complete 
        if(valid):
            dataTransfer(connection_socket,socket_poll,cid,100,filename,0,1000)

    else:
        #invalid connection 
        return


# data trasfer loop 
def dataTransfer(connection_socket,socket_poll,cid,packetsize,filename,file_offset,bufferlength):
    global client_Endpoint

    #Start loading data from file and consturct packets
    fd = open(filename.decode("utf-8"),"rb")
    data_thread = datathread.data_packet_queue(cid,packetsize,fd,file_offset,bufferlength)
    #Data will be read from the file into data_thread.data_packets
    data_thread.start()

    flow_control = rft_flow_control.flow_control(data_rate)
    congestion_control = rft_congestion_control.rft_congestion_control()
    send_and_unacked_packets = dict()

    file_send = False
    highest_offset_send = 0
    lowest_acked_offset = 0
    packet_sent = 0 
    #TODO: congestion/flow control,...

    #congestion control
    cwnd = 1
    swnd = cwnd
    ssthresh = 10
    ###################

    round = 0
    while(True):
        # flow_control 
        time.sleep(0.1)
        # print('round:',round)
        # handle file changed  
        if not checkfile(filename):
            #send statuscode
            send_status(client_Endpoint,cid,connection_socket,rft_packet.rft_status_codes.File_changed,"File changed")
            print('File changed , transfer termination')
            return
        #Send data
        if(len(data_thread.data_packets)>0):
            
            # sending_queue = collections.deque()
            # w_max = min(cwnd,len(data_thread.data_packets))
            # c = w_max 
            # while c > 0:
            #     packet = data_thread.data_packets.pop()
            #     sending_queue.appendleft(packet)
            #     c -= 1
            # print(sending_queue)
            # while len(sending_queue) > 0 :
            #     packet == sending_queue.pop()
            #     print (packet)
            #     print(sending_queue)
            #     if packet.isFin():
            #         connection_socket.sendto(bytes(packet),client_Endpoint)
            #         file_send = True
            #         print('file:{0} send complete'.format(fd))
            #         return
            #     # print('packet:{0} send '.format(packet.file_offset))
            #     connection_socket.sendto(bytes(packet),client_Endpoint)

            
            packet = data_thread.data_packets.pop()
            # print(packet)
            # print('sending buffer length: ',len(data_thread.data_packets))
            # handle last packet 
            if packet.isFin():
                connection_socket.sendto(bytes(packet),client_Endpoint)
                file_send = True
                print('file:{0} send complete'.format(fd))
                return

            # if(flow_control.flow_control(len(bytes(packet))) and congestion_control.congeston_control(len(bytes(packet)))):
#########################################send to Client##################################
            #packet loss simulation
            # if(packet.getFileoffset() == 500 and round == 0 or packet.getFileoffset() == 800 and round == 0 ):
            # # if(packet.getFileoffset() == 500 and round == 0) :
            #     packet_sent += 1
            #     highest_offset_send = max(packet.getFileoffset(),highest_offset_send)
            #     send_and_unacked_packets[packet.getFileoffset()] = (packet, 0, timestamp())
            #     continue

            connection_socket.sendto(bytes(packet),client_Endpoint)

            # packet_sent += w_max   
            highest_offset_send = max(packet.getFileoffset(),highest_offset_send)
            send_and_unacked_packets[packet.getFileoffset()] = (packet, 0, timestamp()) #0 -- Packet needs to be acked 1 -- Packet in nack range 2 -- timeout packet
            # print('send and unacked packet: ',send_and_unacked_packets)
            print(int.from_bytes(packet.file_offset,byteorder="big")) #For testing
            # else:
            #     data_thread.append(packet)
            loss = 0
            missing = list()

            event_list = socket_poll.poll(0)
            #awaiting for an incoming packet
            for fd, event in event_list:
                if(fd == connection_socket.fileno()):
                    
                    data, src = connection_socket.recvfrom(1500)
                    packet = rft_packet.rft_packet(data)

                    #TODO: STC, cid,Fin without missing ranges or status codes ...
                    #assertions for connection ID client address

    #############################################handle incoming packet#################################
                   
                    print('receive a packet from : ',src)
                    # print(packet)
                    # connection migration with current CID
                    if(src != client_Endpoint):
                        if(packet.getCID()==cid):
                            print('connection may lost, reconnecting...')
                            client_Endpoint = src
                            continue
                        else :
                            print('connection lost , CID mismatch, terminate.')
                            send_status(client_Endpoint,packet.getCID(),connection_socket,rft_packet.rft_status_codes.Unknown,"connection migration fail")
                            return

                    if(packet.getCID()!=cid):
                        send_status(client_Endpoint,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"CID not matching")
                        # terminate connection with current session 
                        return

                    if(packet.isDtr()):
                        print(packet)
                        flow_control.change_flow_control_data_rate(packet.data_rate)

                    if(packet.isStc()):
                        print(packet)
                        status = packet.get_status_code()
                        if(status == rft_packet.rft_status_codes.File_not_available or status == 4 or status == 5):
                            return
                        if(status == rft_packet.rft_status_codes.Connection_Terminated):
                            print('ACK STC Term')
                            return
                        if(status == rft_packet.rft_status_codes.File_changed):
                            #never happen 
                            return

                        #For any status code or status code acks that were not expected/not defined
                        send_status(client_Endpoint,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"Unkown status code/Different error")
                        return

    ######################################################################################################                      
                   #handle ack
                    nacks = packet.get_nack_ranges()
                    print('get nackrange:',nacks)
                    if(nacks):
                        #TODO: Change to 500->512
                        lowest_acked_offset = nacks[0][0]
                        print("lowest_acked_offset : ",nacks[0][0])
                        for nack in nacks:
                            # print(send_and_unacked_packets)
                            i = nack[0]
                            while(i<nack[1]):
                                packet, state, t = send_and_unacked_packets[i]
                                # print("get missing packet .... retransfer: ", packet)
                                send_and_unacked_packets[i] = (packet,1,timestamp())
                                if(state==0):
                                    missing.append(packet)
                                elif(state == 2):
                                    loss += 1
                                i += packetsize
                            #send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if not_acked <= v[0].getFileoffset() }

                        loss += len(missing)
                    else:
                        # no transfer loss 
                        lowest_acked_offset = packet.getFileoffset()
                        print("Update lowest acked offset to {0}".format(lowest_acked_offset))
                    round+=1
                    send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if not_acked >=lowest_acked_offset}
                    # print(send_and_unacked_packets)

#Handle timeout
            # offsets = send_and_unacked_packets.keys()
            # current_time = timestamp()
            # for offset in offsets:
            #     if(offset<=lowest_acked_offset):
            #         continue

            #     packet, state, t = send_and_unacked_packets[offset]
            #     if(state == 1 or state == 2):
            #         continue
            #     if(current_time-t >= rft_packet.timeout_time):
            #         send_and_unacked_packets[offset] = (packet,2,current_time)
            #         missing.append(packet)


            if missing:
                # print('missing list :',missing)
                missing.reverse()
                # adding to sending buffer 
                data_thread.data_packets.extend(missing)
                # congestion_control.update_packet_loss(len(missing) / (highest_offset_send - lowest_acked_offset))
                # print(data_thread.data_packets)
        # only receiveing data 
        else:
            print('something went wrong')
            return
        cwnd += 1
        round += 1



def grace_exithandler(socket,CID,signal_received, frame):
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    termConnection(socket,CID,client_Endpoint)

def termConnection(socket,CID,address):
    s = 'connection terminated!'
    print(s)
    termination_packet = rft_packet.rft_packet.create_status(CID,rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
    # print(termination_packet)
    socket.sendto(bytes(termination_packet),address)
    serverDown()

# free all resources 
def serverDown():
    print('server shutdown')
    exit(0)

def grave_termServer(signal_received, frame):
    serverDown()
    
def server(port,p,q):
    #Socket creation for IPv4/IPv6
    try:
        ipv4_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ipv6_socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        ipv4_socket.bind(("127.0.0.1",port))
        ipv6_socket.bind(("::1",port))

    except Exception:
        print("Something went wrong while trying to create the sockets")
        return

    print("Listening on Port {0} ".format(port))

    #Create Poll object
    socket_poll = select.poll()
    socket_poll.register(ipv4_socket, select.POLLIN)
    socket_poll.register(ipv6_socket, select.POLLIN)

    #Wait for connections
    while(True):
        signal(SIGINT,grave_termServer)
        print("Waiting for incomming connections")
        event_list = socket_poll.poll()
        for fd, event in event_list:
            if(fd == ipv6_socket.fileno()):
                connection_loop(ipv6_socket)

            elif(fd == ipv4_socket.fileno()):
                connection_loop(ipv4_socket)
