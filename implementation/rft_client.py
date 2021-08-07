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
from pathlib import Path
from signal import signal, SIGINT
from sys import exit
from functools import partial






#Deal with the inital Handshake #TODO: Handel Status messages
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

                if(server_response.isNew()):
                    client_response = rft_packet.rft_packet.create_client_hello_ack(server_response.getCID())
                    client_socket.sendto(bytes(client_response),server_addr)
                    handshake_not_done = False
                    #Handshake done 
                    print('Handshake "completed"')
                    return (True,client_response.getCID(),server_response.payload,client_response)
        tries -= 1
    
    return (False,None,None,None)


def grace_exithandler(socket,CID,signal_received, frame):
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    termConnection(socket,CID,server_addr)



def termConnection(client_socket,CID,address):
    s = 'connection terminated!'
    print(s)
    termination_packet = rft_packet.rft_packet.create_status(CID,rft_packet.rft_status_codes.Connection_Terminated,rft_packet.STC,s)
    # print(termination_packet)
    client_socket.sendto(bytes(termination_packet),address)
    exit(0)




#End point info

dst_addr = "127.0.0.1"
file_name  = "test.png"
port = 8888
home_port = 1111

def connectionloop(client_socket,files):
    # global CID 
    # global cl_socket 
    

    socket_poll = select.poll()
    socket_poll.register(client_socket, select.POLLIN)

    #Handle handshake last_handshake_packet in case it is lost and needs to be resend
    file_ok, cid, checksum, last_handshake_packet  = exchange_handshake(socket_poll,client_socket,files)
    

    signal(SIGINT,partial(grace_exithandler,client_socket,cid))
    
    handshake_done = False
    #Go into the transfer of the first file
    #TODO: Multiple files, flow control use (send dtr, maybe use queue to determine the new data rate)
    if(file_ok):
        # Check if file exists
        if Path(files).is_file():
            print ("File {0} exists , Overwrite...".format(files))

        file = open(files,"wb")
        write_thread = datathread.data_write_queue(file)
        
        max_ack = 0
        packets_received = 0
        while(True):
            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if(fd == client_socket.fileno()):
                    data, src = client_socket.recvfrom(1500)
                    packet = rft_packet.rft_packet(data)
                    # assert serveraddr = receivedaddr 

############################################################################################
                    # not necessarily
                    #Handshake completion in case the last ACK is lost
                    if(not handshake_done):
                        if(packet.isNew()):
                            client_socket.sendto(bytes(client_response),server_addr)
                            continue
                        else:
                            handshake_done = True
                    
#############################################################################################
                    
                    #TODO: packet checks,status codes,...

                    # status code check before write packet to buffer 

                    write_thread.add(packet)
                    packets_received += 1
                    #update ACK
                    max_ack = max(packet.getFileoffset()+len(packet.payload),max_ack)
                    #TODO: Remove for testing only
                    print(int.from_bytes(packet.file_offset,byteorder="big"))
                    write_thread.write()
                    
                     #check if packet is FIN 
                    if(packet.isFin()):
                        print("ACK_FIN")
                        fin_packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.FIN)
                        print(fin_packet)
                        return



            #TODO: after timeout ack
            if(packets_received>29):
                print("ACK")
                packet = rft_packet.rft_packet.create_ack_packet(cid,max_ack,rft_packet.ACK,nack_ranges = write_thread.get_missing_ranges())
                client_socket.sendto(bytes(packet),(dst_addr,port))
                packets_received = 0


                    
            
def client(host,port,p,q,files):
    #Check for input address IPv4/IPv6
    IPv4 = True
    #for test 
    global server_addr 
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
        connectionloop(client_socket,files[i])

