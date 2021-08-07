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



def complete_handshake(connection_socket,tries=3,timeout=10):
    global data_rate,client_addr
    #Receive handshake message from client 
    data, client_addr = connection_socket.recvfrom(1500)
    print('packet received , parsing...')
    packet = rft_packet.rft_packet(data)
    
    if(packet.getlength()>0):

        file_name = packet.payload
        # handle invalid response length may never executed 
        if(len(data)<packet.getlength()): #TODO: Does not make sense; replace with actual length check
            print("Invalid length in first Handshake packet.\nDropping connection")
            send_status(client_addr,connection_socket,type,0,"Invalid length in first Handshake packet.")
            return (None, False, None, None)
        #Check handshake flags...
        #TODO:Add resumption case


        # handle invalid NEW packet from client 
        if(not packet.isNew() or packet.getCID() != 0 or packet.getFileoffset() != 0):
            # print(packet.flags,packet.cid,packet.getFileoffset())
            print("Invalid handshake packet")
            send_status(client_addr,connection_socket,0,"New not set or CID or Fileoffset not 0")
            return (None, False, None, None)

        #Everything good so far 
        # check if DTR is set 
        if(packet.isDtr()):
            data_rate=packet.dtr

        #Try loading the file
        try:
            
            file = open(file_name.decode("utf-8"),"rb")
        # handle file not avaliable
        except FileNotFoundError:
            #File not found send back status to terminate connection
            send_status(client_addr,connection_socket,rft_packet.rft_status_codes.File_not_available,"File not found")
            print("File not found")
            print(file_name.decode("utf-8"))
            return (None, False, None, None)

        
        checksum = hashlib.sha256(open(file_name.decode("utf-8"),"rb").read()).digest()

        #Consturct the Server Hello to send back to the client
        CID = addConnectionID()
        server_packet = rft_packet.rft_packet.create_server_handshake(CID,checksum) #TODO: Better CID
        print('calculate checksum of request file : {0} now sending server hello to client : {1}'.format(checksum,client_addr[0]) )
        #Send the packet to the client and wait for a response
        connection_socket.sendto(bytes(server_packet),client_addr)

        #Wait for answer or resend
        socket_poll = select.poll()
        socket_poll.register(connection_socket, select.POLLIN)
        handshake_done = False
        
        while(handshake_done==False and tries>0):
            # Polls the set of registered file descriptors, and returns a possibly-empty list containing (fd, event) 
            event_list = socket_poll.poll(timeout)

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
                    src_addr = src
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

                    handshake_done = True
                    break
                    #Ok, everything is correct 
                    

    return (file, handshake_done, CID,client_addr,socket_poll)


def timestamp():
    return datetime.datetime.now().timestamp()


def connection_loop(connection_socket):

    fd, valid, cid, src_addr, socket_poll = complete_handshake(connection_socket)

    #Handshake complete 
    if(valid):
        dataTransfer(connection_socket,socket_poll,cid,500,fd,1000)

    else:
        #Something went wrong
        return


# data trasfer loop 
def dataTransfer(connection_socket,socket_poll,cid,packetsize,fd,bufferlength):
    #Start loading data from file and consturct packets
    data_thread = datathread.data_packet_queue(cid,packetsize,fd,bufferlength)
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
        time.sleep(0.05)
        #Send data
        if(len(data_thread.data_packets)>0):
            
            # handle last packet 
            if(len(data_thread.data_packets)==1):
                #lastpacket 
                packet = data_thread.data_packets.pop()
                # add fin flag to the last packet and termination 
                packet.flags = packet.flags | rft_packet.FIN
                connection_socket.sendto(bytes(packet),client_addr)
                print('file:{0} send complete'.format(fd))
                return
            

            packet = data_thread.data_packets.pop()
            if(flow_control.flow_control(len(bytes(packet))) and congestion_control.congeston_control(len(bytes(packet)))):
                connection_socket.sendto(bytes(packet),client_addr)
                highest_offset_send = max(packet.getFileoffset(),highest_offset_send)
                send_and_unacked_packets[packet.getFileoffset()] = (packet, 0, timestamp()) #0 -- Packet needs to be acked 1 -- Packet in nack range 2 -- timeout packet
                print(int.from_bytes(packet.file_offset,byteorder="big")) #For testing

            else:
                data_thread.append(packet)
            #TODO: Test  
            loss = 0
            missing = list()

            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if(fd == connection_socket.fileno()):

                    #receiving ACK 
                    data, src = connection_socket.recvfrom(1500)
                    src_addr = src
                    packet = rft_packet.rft_packet(data)

                    #TODO: STC, cid,Fin without missing ranges or status codes ...
                    
                    if(src!=src_addr and packet.getCID()!=cid):
                        continue
                    
                    if(packet.getCID()==cid):
                        src_addr = src

                    if(packet.getCID()!=cid):
                        send_status(src_addr,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"CID not matching")
                        return

                    if(packet.isDtr()):
                        flow_control.change_flow_control_data_rate(packet.data_rate)

                    if(packet.isStc()):
                        status, msg = packet.get_status_code()
                        if(status == 2 or status == 4 or status == 5):
                            return
                        if(status == 1):
                            #TODO: ack
                            return
                        #For any status code or status code acks that were not expected/not defined
                        send_status(src_addr,cid,connection_socket,rft_packet.rft_status_codes.Unknown,"Unkown status code/Different error")
                        return
                        

                            

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
            
            print('file:{0} send complete'.format(fd))
            return




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
        print("Waiting for incomming connections")
        event_list = socket_poll.poll()
        for fd, event in event_list:
            if(fd == ipv6_socket.fileno()):
                connection_loop(ipv6_socket)

            elif(fd == ipv4_socket.fileno()):
                connection_loop(ipv4_socket)









#server(1111,0,0)