#########################################
#       RFT Server Implementation       #
#                                       #
#                                       #
#########################################


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
from signal import signal, SIGINT
from sys import exit
from functools import partial


data_rate = 0
# client adress
client_addr = None

#Other acks are ignored since the status code have a higher priority (reset connections)
#TODO: ONlY wait for connection terminated acks
def send_status(dst,cid,sock,type,msg,tries=-1,timeout=10):
    global src_addr
    status_packet = rft_packet.rft_packet.create_status(cid,type,rft_packet.STC,msg)
    ack_received = False
    socket_poll = select.poll()
    socket_poll.register(sock, select.POLLIN)
    while(tries>=0 and not ack_received):

        sock.sendto(bytes(status_packet),dst)

        event_list = socket_poll.poll(timeout=timeout)
    
        if(not event_list):
            tries -= 1
            sock.sendto(bytes(sock),dst)
            
        for fd, event in event_list:
            if(fd == sock.fileno()):
                data, src = sock.recvfrom(1500)
                src_addr = src
                packet = rft_packet.rft_packet(data)
                #TODO: check if it is the ack for the stc
                if(not packet.isAck() and not packet.isStc()):
                    continue
                else:
                    ack_received = True

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

        if not openfile(file_name,connection_socket) :
            return (None, False, None, None,None)
         
        file = open(file_name.decode("utf-8"),"rb")

        checksum = hashlib.sha256(open(file_name.decode("utf-8"),"rb").read()).digest()

        #Consturct the Server Hello to send back to the client
        CID = addConnectionID()
        server_packet = rft_packet.rft_packet.create_server_handshake(CID,checksum) 
        # print('calculate checksum of request file : {0} now sending server hello to client : {1}'.format(checksum,client_addr[0]) )
        #Send the packet to the client and wait for a response
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
                    
                    #Check if src changed
                    # assert src == client_addr
                    client_addr = src
                    # check the vallidity of client respond
                    if(packet.isStc()):
                        #Any Status code in the handshake is a problem 
                        code, msg = packet.get_status_code()
                        status_ack = rft_packet.rft_packet.create_status(CID,code,rft_packet.STC|rft_packet.ACK)
                        connection_socket.sendto(bytes(status_ack),src_addr)
                        print("Status code {0} receivedin handshake phase , Invalid {1}".format(code,msg))
                        return (None, False, None, None)

                    if(not packet.isNew() and not packet.isAck() or not packet.getCID() in CIDs or packet.getFileoffset() != 0): 
                        print(packet.flags,packet.cid,packet.getFileoffset())
                        print("Invalid handshake packet")
                        send_status(src,connection_socket,rft_packet.rft_status_codes.Unknown,"New or ACK not set or CID is not cid or Fileoffset not 0")
                        return (None, False, None, None)


                    if(packet.isDtr()):
                        data_rate=packet.dtr
                        print(data_rate)
                        print(packet)

                    handshake_done = True
                    break
                    #Ok, everything is correct 
                    

    return (file, handshake_done, CID,client_addr,socket_poll)




def timestamp():
    return datetime.datetime.now().timestamp()


def openfile(filename,connection_socket): 
    #try to open the file 
    try:
        file = open(filename.decode("utf-8"),"rb")
        return True
    #handle file not avaliable
    except FileNotFoundError:
    #File not found send back status to terminate connection
        send_status(client_addr,0,connection_socket,rft_packet.rft_status_codes.File_not_available,"File not found")
        print("File not found")
        print(filename.decode("utf-8"))
        return  False

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
        if not openfile(file_name,connection_socket) :
            raise FileNotFoundError("file not found")

        file = open(file_name.decode("utf-8"),"rb")
        checksumcheck = hashlib.sha256(file.read()).digest()
        if checksumcheck != checksum:
            #send status code 
            return

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
            print('resumption handshake done')
            return file,handshake_done,CID,FO
        else : 
            return None,False,None, None
 
def connection_loop(connection_socket):
    global client_addr
    # analyzing incoming data 
    
    socket_poll = select.poll()
    socket_poll.register(connection_socket, select.POLLIN)

    data, addr = connection_socket.recvfrom(1500)
    client_addr = addr
    print('packet received , parsing...')
    packet = rft_packet.rft_packet(data)

    if packet.isRes():
    #is a resume handshake packet!
        try:
            print(packet)
            file,file_ok,cid,fileoffset = resumption_handshake(packet,socket_poll,connection_socket,client_addr)
            signal(SIGINT,partial(grace_exithandler,connection_socket,cid))
            print(file)
            print(file_ok)
            print(cid)
            print(fileoffset)
            if(file_ok):
                dataTransfer(connection_socket,socket_poll,cid,500,file,fileoffset,1000)

        except FileNotFoundError :
            return

       

    if packet.isNew(): 
        #is a new handshake packet! 
        fd, valid, cid, src_addr, socket_poll = complete_handshake(packet,connection_socket,client_addr)

        signal(SIGINT,partial(grace_exithandler,connection_socket,cid))
        #Handshake complete 
        if(valid):
            dataTransfer(connection_socket,socket_poll,cid,500,fd,0,1000)

    else:
        #invalid connection 
        return


# data trasfer loop 
def dataTransfer(connection_socket,socket_poll,cid,packetsize,fd,file_offset,bufferlength):
    #Start loading data from file and consturct packets
    data_thread = datathread.data_packet_queue(cid,packetsize,fd,file_offset,bufferlength)
    #Data will be read from the file into data_thread.data_packets
    data_thread.start()

    flow_control = rft_flow_control.flow_control(data_rate)
    congestion_control = rft_congestion_control.rft_congestion_control()
    send_and_unacked_packets = dict()


    file_send = False
    highest_offset_send = 0
    lowest_acked_offset = 0
    #TODO: congestion/flow control, next file request, connection termination, change file offset from client side...
    #Starting to send data and receive acks
    while(True):
        # flow_control 
        time.sleep(0.2)

        # handle file changed  

        #Send data
        if(len(data_thread.data_packets)>0):
            
            packet = data_thread.data_packets.pop()
            
            # handle last packet 
            if packet.isFin():
                connection_socket.sendto(bytes(packet),client_addr)
                print('file:{0} send complete'.format(fd))
                return

            # if(flow_control.flow_control(len(bytes(packet))) and congestion_control.congeston_control(len(bytes(packet)))):
            connection_socket.sendto(bytes(packet),client_addr)
            highest_offset_send = max(packet.getFileoffset(),highest_offset_send)
            send_and_unacked_packets[packet.getFileoffset()] = (packet, 0, timestamp()) #0 -- Packet needs to be acked 1 -- Packet in nack range 2 -- timeout packet
            print(int.from_bytes(packet.file_offset,byteorder="big")) #For testing

            # else:
            #     data_thread.append(packet)
            loss = 0
            missing = list()

            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if(fd == connection_socket.fileno()):
                    


                    #receiving ACK 
                    data, src = connection_socket.recvfrom(1500)
                    # assert true 
                    # src_addr == src
                    packet = rft_packet.rft_packet(data)



                    #TODO: STC, cid,Fin without missing ranges or status codes ...
                    #assertions for connection ID client address

#############################################handle incoming packet#################################
                    if(src!=client_addr and packet.getCID()!=cid):
                        print('connection corrupted')
                        # terminate connection with current session
                        return

                    if(packet.getCID()==cid):
                        src_addr = src

                    if(packet.getCID()!=cid):
                        send_status(src_addr,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"CID not matching")
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
                            #TODO: ack
                            print('TODO ACK')
                            return
                        #For any status code or status code acks that were not expected/not defined
                        send_status(src_addr,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"Unkown status code/Different error")
                        return

 ######################################################################################################                       
                    nacks = packet.get_nack_ranges()
                    
                    if(nacks):
                        #TODO: Change to 500->512
                        lowest_acked_offset = nacks[0][0]
                        for nack in nacks:
                            i = nack[0]
                            while(i<nack[1]):
                                packet, state, t = send_and_unacked_packets[i]
                                send_and_unacked_packets[i+500] = (packet,1,timestamp())
                                if(state==0):
                                    missing.append(packet)
                                elif(state == 2):
                                    loss += 1
                                i += 500
                            #send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if not_acked <= v[0].getFileoffset() }

                        loss += len(missing)
                    else:
                        lowest_acked_offset = packet.getFileoffset()

                    send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if not_acked <= v[0].getFileoffset() }

#Handle timeout
#Can be ignored
            offsets = send_and_unacked_packets.keys()
            current_time = timestamp()
            for offset in offsets:
                if(offset<=lowest_acked_offset):
                    continue

                packet, state, t = send_and_unacked_packets[offset]
                if(state == 1 or state == 2):
                    continue
                if(current_time-t >= rft_packet.timeout_time):
                    send_and_unacked_packets[offset] = (packet,2,current_time)
                    missing.append(packet)


            if(missing and missing is not None):

                # data_thread.data_packets.extend(missing.reverse())
                congestion_control.update_packet_loss(len(missing)/(highest_offset_send-lowest_acked_offset))
        else:
            
            print('something went wrong')
            return

def grace_exithandler(socket,CID,signal_received, frame):
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    termConnection(socket,CID,client_addr)

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
    
    print("Listining on Port {0} and {1}".format(port,port))

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
