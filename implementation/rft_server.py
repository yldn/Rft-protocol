#########################################
#       RFT Server Implementation       #
#                                       #
#                                       #
#########################################
import base64
import collections
from enum import Enum
import os
from sys import path

import rft_packet
import rft_congestion_control
import socket
import select
import hashlib
import datathread
import time
import rft_flow_control
import datetime
import logging
class sState(Enum):
    nLost = 1
    nNotLost = 2
logger = None

data_rate = 0
src_addr = None
import random
p=None
q=None
data_thread = None


v = False
recv = False
show_cwnd = False
show_nacks = False

simulationStateServer = sState.nNotLost
show_all = False
timeout_timer = 0
listfiles = False

def timestamp():
    return datetime.datetime.now().timestamp()

def simulationSendServer(connection_socket, server_packet):
    global q,p,simulationStateServer
    if simulationStateServer == sState.nLost:
        if random.random() <= q:
            pass
        else:
            connection_socket.sendto(bytes(server_packet), src_addr)
            simulationSendServer = sState.nNotLost
            if(not v):
                logger.debug(server_packet.simple_str())            
            else:
                logger.debug(server_packet)
            # print("Server side send:")
            # print(server_packet)
            #print(int.from_bytes(server_packet.file_offset, byteorder="big"))  # For testing
    elif simulationStateServer ==sState.nNotLost:
        if random.random() <= p:
            simulationSendServer = sState.nLost
        else:
            connection_socket.sendto(bytes(server_packet), src_addr)

            if(not v):
                logger.debug(server_packet.simple_str())            
            else:
                logger.debug(server_packet)
            # print("Server side send:")
            # print(server_packet)
            #print(int.from_bytes(server_packet.file_offset, byteorder="big"))  # For testing
# Other acks are ignored since the status code have a higher priority (reset connections)

def send_status(dst, cid, typeCode, sock, msg, tries=-1, timeout=10):
    global src_addr
    status_packet = rft_packet.rft_packet.create_status(cid, typeCode, rft_packet.STC, msg)
    ack_received = False
    socket_poll = select.poll()
    socket_poll.register(sock, select.POLLIN)
    simulationSendServer(sock, status_packet)
    while (tries > -1 and not ack_received):

        #sock.sendto(bytes(status_packet), dst)
        simulationSendServer(sock, status_packet)

        event_list = socket_poll.poll(timeout)

        if (not event_list):  # error variables so changed it might be wrong
            tries -= 1
            #sock.sendto(bytes(status_packet), src_addr)
            simulationSendServer(sock, status_packet)
        for fd, event in event_list:
            if (fd == sock.fileno()):
                data, src = sock.recvfrom(1500)
                src_addr = src
                packet = rft_packet.rft_packet(data)
                # TODO: check if it is the ack for the stc
                if (not packet.isAck() and not packet.isStc()):
                    continue
                else:
                    ack_received = True

    return ack_received
    pass


def findAUniqueFreshCID(usedCIDs):
    x = (random.getrandbits(32))
    while x in usedCIDs:
        x = (random.getrandbits(32))
    usedCIDs.append(x)
    return x


def sendUnknownStatusError(packet, connection_socket, src):
    #print(packet.flags, packet.cid, packet.getFileoffset())
    logger.warning("Invalid handshake packet")
    send_status(src, packet.getCID(), rft_packet.rft_status_codes.Unknown, connection_socket,
                "New not set or CID or Fileoffset not 0")

# to indecate if it's a RES request or NEW request
def answerFileRequest(connection_socket, usedCIDs, tries=3, timeout=100):
    global data_rate, src_addr
    # Receive data
    data, src = connection_socket.recvfrom(1500)
    global src_addr
    src_addr = src
    packet = rft_packet.rft_packet(data)
    handshake_done = False

    # Check handshake flags...
    if not (packet.isNew() or packet.isRes()) or packet.getCID() != 0:
        sendUnknownStatusError(packet, connection_socket, src)
        return None, False, None, None, None, None

    #deprecated
    # TODO: Does not make sense; replace with actual length check  Done via rfc_packet lengthCheck IS used
    # if  rft_packet.rft_packet.lengthCheck(packet,isSendByServer =False):
    #     print("Invalid length in first Handshake packet.\nDropping connection")
    #     send_status(src,packet.getCID(),   rft_packet.rft_status_codes.Unknown,connection_socket,  "Invalid length in first Handshake packet.")

    # Everything good so far
    if packet.isDtr():
        data_rate = packet.dtr

    freshCID = findAUniqueFreshCID(usedCIDs)

    # New File  Request
    if packet.isNew() and packet.getCID() == 0:
        if packet.getFileoffset() != 0:
            sendUnknownStatusError(packet, connection_socket, src)
        if (packet.getlength() > 0):
            file_name = packet.payload
            # Try loading the file
            try:
                logger.debug("Answer File Request")
                if v:
                    logger.debug(packet)
                else:
                    logger.debug(packet.simple_str())
                logger.info("trying: {0}".format(file_name.decode("utf-8")))
                
                file = open(file_name.decode("utf-8"), "rb")
                
            except FileNotFoundError:
                # File not found send back status to terminate connection
                send_status(src, freshCID, rft_packet.rft_status_codes.File_not_available, connection_socket,
                            "File not found"+" "+file_name.decode("utf-8"))
                logger.warning("File not found: {0}".format(file_name.decode("utf-8")))
                return None, False, None, None, None, None
            checkSum = hashlib.sha256(open(file_name, "rb").read()).digest()
            return complete_handshake(freshCID, checkSum, src, connection_socket, tries, timeout,
                                      file)  # FILE OFFSET SHOULD BE 0

        return None, False, None, None, None, None

    # File Resumption Request
    if packet.isRes() and packet.getCID() == 0:
        # no need to check file offset being 0, after handshake it can continue
        isCheck, file =resumeCheckSumCheck(packet,src,freshCID,connection_socket)
        if  isCheck:
            logger.info("Starting resume handshake")
            return resume_handshake(freshCID, packet.checksum, packet.getFileoffset(), src, connection_socket, tries,
                                    timeout, file)
        else:
            return None, False, None, None, None, None

def resumeCheckSumCheck(packet,src,freshCID,connection_socket):
    received_checksum = packet.checksum
    file_name = packet.filename
    # Try loading the file
    try:
        file = open(file_name.decode("utf-8"), "rb")

    except FileNotFoundError:
        # File not found send back status to terminate connection
        send_status(src, freshCID, rft_packet.rft_status_codes.File_not_available, connection_socket,
                    "File not found")
        logger.warning("File not found {0}".format(file_name.decode("utf-8")))
        return False,None
    calculatedChecksum = hashlib.sha256(open(file_name, "rb").read()).digest()

    if calculatedChecksum != received_checksum:
        send_status(src, freshCID, rft_packet.rft_status_codes.File_changed, connection_socket,
                    "Checksums do not match")
        logger.warning("File changed: {0}".format(file_name.decode("utf-8")))
        return False,None
    else:
        return True,file

def resume_handshake(freshCID, checksum, givenOffset, src, connection_socket, tries, timeout, file):
    handshake_done = False
    # Construct the Server Hello to send back to the client
    server_packet = rft_packet.rft_packet.create_server_resumption_handshake(freshCID, checksum, givenOffset)
    # Save src address
    #print(server_packet)
    c_src = src

    # Send the packet to the client and wait for a response
    #connection_socket.sendto(bytes(server_packet), c_src)
    simulationSendServer(connection_socket, server_packet)
    # Wait for answer or resend
    socket_poll = select.poll()
    socket_poll.register(connection_socket, select.POLLIN)
    timeout_time = timestamp()

    while handshake_done == False and (timestamp()-timeout_time < 10):
        event_list = socket_poll.poll(timeout)

        if not event_list:
            tries -= 1
            #connection_socket.sendto(bytes(server_packet), c_src)
            simulationSendServer(connection_socket, server_packet)

        for fd, event in event_list:
            if fd == connection_socket.fileno():
                data, src = connection_socket.recvfrom(1500)
                # print(rft_packet.rft_packet(data))
                packet = rft_packet.rft_packet(data)

                # Check if src changed
                src_addr = src

                if packet.isStc():
                    # Any Status code in the handshake is a problem
                    code, msg = packet.get_status_code()
                    status_ack = rft_packet.rft_packet.create_status_ack_packet(freshCID, packet.getFileoffset(), code,msg)
                    simulationSendServer(connection_socket,status_ack)
                    logger.info("Status code {0} received: {1}".format(code, msg))
                    return None, handshake_done, None, None, None, None

                if not packet.isRes() and not packet.isAck() or packet.getCID() != freshCID or givenOffset != packet.getFileoffset():
                    #print(packet.flags, packet.getCID(), packet.getFileoffset())
                    logger.warning("Invalid Resumption handshake packet")
                    #print(packet)
                    send_status(src, freshCID, rft_packet.rft_status_codes.Unknown, connection_socket,
                                "RES or ACK not set or CID is not cid or not include the desired offset")
                    return None, handshake_done, None, None, None, None
                # TODO:  data rate not used
                if packet.isDtr():
                    data_rate = packet.dtr

                handshake_done = True
                return file, handshake_done, freshCID, c_src, socket_poll, givenOffset

                # Ok, everything is correct

    return None, handshake_done, None, None, None, None


def complete_handshake(freshCID, checksum, src, connection_socket, tries, timeout, file):
    handshake_done = False
    # Construct the Server Hello to send back to the client
    server_packet = rft_packet.rft_packet.create_server_handshake(freshCID, checksum)
    # Save src address
    c_src = src
    # Send the packet to the client and wait for a response
    #connection_socket.sendto(bytes(server_packet), c_src)
    simulationSendServer(connection_socket, server_packet)
    # Wait for answer or resend
    socket_poll = select.poll()
    socket_poll.register(connection_socket, select.POLLIN)
    timeout_time = timestamp()
    while handshake_done == False and (timestamp()-timeout_time < 10):
        event_list = socket_poll.poll(timeout)

        if not event_list:
            tries -= 1
            #connection_socket.sendto(bytes(server_packet), c_src)
            simulationSendServer(connection_socket, server_packet)
        for fd, event in event_list:
            if fd == connection_socket.fileno():
                data, src = connection_socket.recvfrom(1500)
                # print(rft_packet.rft_packet(data))
                packet = rft_packet.rft_packet(data)
                if v:
                    logger.debug(packet)
                else:
                    logger.debug(packet.simple_str())
                # Check if src changed
                src_addr = src

                if packet.isStc():
                    # Any Status code in the handshake is a problem
                    code, msg = packet.get_status_code()
                    # status_ack = rft_packet.rft_packet.create_status(freshCID,code,rft_packet.STC|rft_packet.ACK) #Todo: during hadnshake src message received
                    # status_ack = rft_packet.rft_packet.create_status_ack_packet(freshCID, packet.file_offset, code,msg)
                    # connection_socket.sendto(bytes(status_ack),src_addr)
                    logger.info("Status code {0} received: {1}".format(code, msg))
                    return None, handshake_done, None, None, None, None

                if not packet.isNew() and not packet.isAck() or packet.getCID() != freshCID or packet.getFileoffset() != 0:
                    #print(packet.flags, packet.cid, packet.getFileoffset())
                    #print(packet)
                    logger.warning("Invalid handshake packet")
                    send_status(src, freshCID,  rft_packet.rft_status_codes.Unknown,connection_socket,
                                "New or ACK not set or CID is not cid or Fileoffset not 0")
                    return None, handshake_done, None, None, None, None
                global data_rate
                if packet.isDtr():
                    data_rate = packet.dtr

                handshake_done = True
                return file, handshake_done, freshCID, c_src, socket_poll, 0
                # Ok, everything is correct

    return None, handshake_done, None, None, None, None




def checkfile(filename):
    if(len(filename)) :
    # print(filename.decode("utf-8"))
        return os.path.isfile(filename)
    else :
        logger.warning('invalid filename')
        return False

restartConnectionLoop=False

#Data transfer
def connection_loop(connection_socket, usedCIDs,src_addrr,fileOFF,fileName,cidd ):
    global restartConnectionLoop,data_thread,src_addr
    if not restartConnectionLoop:
         fd, valid, cid, src_addr, socket_poll, fileOffset = answerFileRequest(connection_socket, usedCIDs)
         if(valid):
            logger.info("Handshake done")
         else:
             logger.info("Handshake failed")
             return
    else:
        logger.info("Restarting connection loop for the next file {0}".format(fileName))
        restartConnectionLoop=False
        try:
            #file = open(fileName.decode("utf-8"), "rb")
            fd =  open(fileName.decode("utf-8"), "rb")
            valid = True
            cid = cidd
            fileOffset = fileOFF
            src_addr = src_addrr
            socket_poll = select.poll()
            socket_poll.register(connection_socket, select.POLLIN)
        except FileNotFoundError:
            # File not found send back status to terminate connection
            send_status(src_addrr, cidd, rft_packet.rft_status_codes.File_not_available, connection_socket,"File not found")
            logger.warning("File not found: {0}".format(fileName.decode("utf-8")))
            return

    # Handshake complete
    if (valid):
        # Start loading data from file and consturct packets
        filename = fd.name
        if(data_thread is not None):
            data_thread.stop = True
        data_thread = datathread.data_packet_queue(cid, rft_packet.payload_size, fd, 1000, fileOffset)
        # Data will be read from the file into data_thread.data_packets
        data_thread.start()
        flow_control = rft_flow_control.flow_control(data_rate)
        
        congestion_control = rft_congestion_control.rft_congestion_control()
        send_and_unacked_packets = dict()
        highest_offset_send = 0
        closeConnection = False
        lowest_acked_offset = 0
        dontSend=False
        timeout_timestamp = datetime.datetime.now().timestamp()
        # Starting to send data and receive acks

        #######################congestion_control####################
        cwnd = int(1) 
        # swnd = cwnd 
        # ssthresh = 10 


        sending_queue = collections.deque()

        while (True):

            if closeConnection:
                break

            # Send data

            if (len(data_thread.data_packets) > 0 and not dontSend):
                if(show_cwnd):
                    logger.debug('current cwnd: {0}'.format(cwnd))
                #congestion control 

                wmax = min(len(data_thread.data_packets),cwnd)
                c=wmax
                while c > 0 and flow_control.flow_control(rft_packet.payload_size): 
                    packet = data_thread.data_packets.pop()
                    send_and_unacked_packets[packet.getFileoffset()] = (packet, 0,
                                                                        timestamp())
                    simulationSendServer(connection_socket,packet)
                    #sending_queue.appendleft(packet)
                    c -= 1 
                    # print(sending_queue)
                    #while len(sending_queue)>0 :
                    #    packet = sending_queue.pop()
                        # print(packet)
            else:
                if False:
                    print("Not sending")
                        

            #if (len(data_thread.data_packets) > 0 and  not dontSend):
            #    packet = data_thread.data_packets.pop()
            #    if (flow_control.flow_control(len(bytes(packet))) and congestion_control.congeston_control(
            #            len(bytes(packet))) ):
                    #print(" data frame to be sent:\n {0}".format(packet.payload))
                    #connection_socket.sendto(bytes(packet), src_addr)
            #        simulationSendServer(connection_socket, packet)
            #        highest_offset_send = max(packet.getFileoffset(), highest_offset_send)
            #        send_and_unacked_packets[packet.getFileoffset()] = (packet, 0,
            #                                                            timestamp())
                    # ack Expectations #0 -- Packet needs to be acked 1 -- Packet in nack range 2 -- timeout packet
                    ##########for testing only when
            #        print(int.from_bytes(packet.file_offset,byteorder="big")) #For testing
            #    else:
            #        data_thread.append(packet)
            #        print("Not sending")
            #        print(flow_control.flow_control(len(bytes(packet))))


            loss = 0
            missing = list()

            #awaiting packets from client...
            event_list = socket_poll.poll(0)
            for fd, event in event_list:
                if (fd == connection_socket.fileno()):
                    # receive ACK if there's one incoming
                    data, src = connection_socket.recvfrom(1500)
                    #src_addr = src
                    packet = rft_packet.rft_packet(data)
                    #print(" ACK received :\n {0}".format(packet.payload))
                    timeout_timestamp = datetime.datetime.now().timestamp()
                    if recv:
                        if v:
                            logger.debug(packet)
                        else:
                            logger.debug(packet.simple_str())
                    if (src != src_addr and packet.getCID() != cid):
                        logger.warning("Packet not valid -- different cid and address")
                        continue

                    if (packet.getCID() == cid):
                        src_addr = src

                    if (packet.getCID() != cid):
                        send_status(src_addr, cid, rft_packet.rft_status_codes.Unknown, connection_socket,
                                    "CID not matching")
                        logger.warning(11)
                        return
                    if (packet.isDtr()):
                        flow_control.change_flow_control_data_rate(packet.data_rate)
                    #handling next filerequest
                    if packet.isFin() and not packet.isStc():

                        if packet.isRes():
                            restartConnectionLoop=True

                            server_packet = rft_packet.rft_packet.create_server_resumption_handshake(cid, packet.checksum,
                                                                                                     packet.getFileoffset())
                            #print(server_packet)
                            # Send the packet to the client and wait for a response
                            #connection_socket.sendto(bytes(server_packet), src_addr)
                            simulationSendServer(connection_socket, server_packet)
                            #checksum check
                            if resumeCheckSumCheck(packet, src, cid, connection_socket):
                                  connection_loop(connection_socket,usedCIDs,src_addr, packet.getFileoffset(), packet.filename,cid)
                            return
                        if packet.isNew():
                            restartConnectionLoop=True
                            fileN = packet.payload

                            #Send File not found in case file is not found and wait for new file request/end of connection
                            try:
                                checkSum = hashlib.sha256(open(fileN, "rb").read()).digest()
                                server_packet = rft_packet.rft_packet.create_server_handshake(cid, checkSum)
                                restartConnectionLoop=True
                            except FileNotFoundError:
                                send_status(src_addr,cid,rft_packet.rft_status_codes.File_not_available,connection_socket,"File not found")
                                dontSend=True

                                continue
                            #print(server_packet)
                            # Send the packet to the client and wait for a response
                            #connection_socket.sendto(bytes(server_packet), src_addr)
                            simulationSendServer(connection_socket, server_packet)

                            return connection_loop(connection_socket,usedCIDs,src_addr,0,fileN,cid)
            
                #############################handle stc###############################
                    if (packet.isStc()):
                        status, msg = packet.get_status_code()
                        logger.info("Status code {0} received: {1}".format(status,msg))
                        if status == rft_packet.rft_status_codes.Timeout or status == rft_packet.rft_status_codes.File_not_available or status == rft_packet.rft_status_codes.Version_mismatch or status == rft_packet.rft_status_codes.Unknown or status == rft_packet.rft_status_codes.File_changed:
                            logger.debug(22)
                            return
                        if status == rft_packet.rft_status_codes.Connection_Terminated:
                            closeConnection = True

                            # Client End of last file: ACK|FIN|STC is received
                            if packet.isAck() and packet.isFin():
                                logger.info("Connection terminated successfully")
                                logger.debug("FIN ACK STC terminate has been received")
                                # Server Status ACK: ACK|STC, CID=est
                                server_response = rft_packet.rft_packet.create_status_ack_packet(
                                    packet.getCID(), packet.getFileoffset(),rft_packet.rft_status_codes.Connection_Terminated,
                                    "Connection ended after all files transfer")
                                #connection_socket.sendto(bytes(server_response), src_addr)
                                simulationSendServer(connection_socket, server_response)
                                #closeConnection = True

                            elif packet.isAck():
                                logger.debug("Just client acking a status message")

                            elif not packet.isAck(): #Client sends a Status message and Server ACKs it
                                server_response = rft_packet.rft_packet.create_status_ack_packet(
                                    packet.getCID(), packet.getFileoffset(),
                                    packet.get_status_code()[0],packet.get_status_code()[1])
                                #connection_socket.sendto(bytes(server_response), src_addr)
                                simulationSendServer(connection_socket, server_response)
                                #closeConnection = True
                            else:
                                logger.warning("Unexpected")

                        # For any status code or status code acks that were not expected/not defined
                        if(not closeConnection):
                            send_status(src_addr, cid, rft_packet.rft_status_codes.Unknown, connection_socket,
                                        "Unkown status code/Different error")
                            logger.debug("44")
                            #print(packet)
                            return
                            
                    if(closeConnection):
                        break #if you do not break get nack ranges thinks payload is nacks that actually STC is
                    nacks = packet.get_nack_ranges()

                    
                    if show_nacks:
                        logger.debug('Missing ranges: {0}'.format(nacks))
                ############################handle Ack ###################
                    nack_list = list()
                    
                    if (nacks):
                        lowest_acked_offset = nacks[0][0]
                        #print("Nacks:",nacks)
                        for nack in nacks:
                            #print(nack)
                            i = nack[0]
                            
                            while (i < nack[1]):
                                old_packet, state, t = send_and_unacked_packets.get(i,(None,None,None))
                                #logger.debug("Packet with offset {0} is going to be retransmitted".format(packet.getFileoffset()))
                                if(old_packet is not None):
                                    #Add highest nack ack
                                    #send_and_unacked_packets[i] = (packet, 1, timestamp())
                                    nack_list.append((i,(old_packet,1,timestamp())))
                                    if (state == 0):
                                        #if data_thread.finalMaxFileOffset > packet.getFileoffset() + len(
                                        #       packet.payload):
                                            missing.append(old_packet)
                                    elif (state == 2):
                                        loss += 1
                                    i += rft_packet.payload_size
                                else:
                                    logger.warning("Invalid Missing Range received")
                                    break
                        #lowest_acked_offset = max(lowest_acked_offset,packet.getFileoffset())
                            # send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if not_acked <= v[0].getFileoffset() }
                        loss += len(missing)
                    else:
                        lowest_acked_offset = max(lowest_acked_offset,packet.getFileoffset())
                        logger.debug("Update lowest acked offset to {0}".format(lowest_acked_offset))
                        #print(packet)
                        # print(send_and_unacked_packets)
                    file_offset = lowest_acked_offset#packet.getFileoffset()
                    send_and_unacked_packets = {not_acked: v for not_acked, v in send_and_unacked_packets.items() if
                                                (not_acked >= file_offset)}
                    #Add packet in nack ranges back to dict
                    for e in nack_list:
                        send_and_unacked_packets[e[0]] = e[1]
                
            # Handle timeout
            offsets = send_and_unacked_packets.keys()
            current_time = timestamp()
            if(data_thread.stop and len(data_thread.data_packets)<50) or True:
                for offset in offsets:
                    if (offset < lowest_acked_offset):
                        continue

                    packet, state, t = send_and_unacked_packets[offset]
                    if (state == 1 or state == 2):
                        continue
                    if current_time - t >= rft_packet.timeout_time:
                        send_and_unacked_packets[offset] = (packet, 2, current_time)
                        #if data_thread.finalMaxFileOffset > packet.getFileoffset()+len(packet.payload):
                        missing.append(packet)
                        # print(packet)
            

            if missing:
                cwnd = cwnd >> 2 
                missing.reverse()
                ##append the missing packets
                data_thread.data_packets.extend(missing)
                # congestion_control.update_packet_loss(len(missing))
                #print('file missing, sending buffer (deque) : ',data_thread.data_packets)

            cwnd += 1  

            if(datetime.datetime.now().timestamp() - timeout_timestamp > 20):
                logger.warning("Timeout")
                send_status(src_addr,cid,rft_packet.rft_status_codes.Unknown,connection_socket,"Timeout")
                return
        
    else:
        # Something went wrong
        #print("Fuck up at line 298")
        return

    pass



def server(port, pGiven, qGiven):
    global p,q, restartConnectionLoop
    p=pGiven
    q=qGiven
    # Socket creation for IPv4/IPv6
    try:
        ipv4_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ipv6_socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        ipv6_socket.setsockopt(socket.IPPROTO_IPV6,socket.IPV6_V6ONLY,True)
        ipv4_socket.bind(("", port))
        ipv6_socket.bind(("", port))

    except Exception:
        logger.error("Something went wrong while trying to create the sockets")
        return

    if(listfiles):
        path='./'
        filestr = 'Avaliable files :\n'
        onlyfiles = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        # filestr += onlyfiles
        for file in onlyfiles:
            if  not file.endswith('.py'): filestr += file + '\n'
        logger.info(filestr)

    logger.info("Listening on Port {0} (IPv4) and {1} (IPv6)".format(port, port))

    # Create Poll object
    socket_poll = select.poll()
    socket_poll.register(ipv4_socket, select.POLLIN)
    socket_poll.register(ipv6_socket, select.POLLIN)
    usedCIDs = list()
    # Wait for connections
    while (True):
        restartConnectionLoop = False
        logger.info("Waiting for incomming connections")
        event_list = socket_poll.poll()
        for fd, event in event_list:
            if (fd == ipv6_socket.fileno()):
                connection_loop(ipv6_socket, usedCIDs,None,None,None,None)

            elif (fd == ipv4_socket.fileno()):
                connection_loop(ipv4_socket, usedCIDs,None,None,None,None)

# server(1111,0,0)
