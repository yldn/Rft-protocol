#########################################
#       RFT Client Implementation       #
#                                       #
#                                       #
#########################################
from datetime import datetime, timedelta
from enum import Enum
from random import random

import rft_packet
import socket
import select
import hashlib
import datathread
import os
from signal import signal, SIGINT
from sys import exit
from functools import partial
import atexit
import time
class sState(Enum):
    nLost = 1
    nNotLost = 2

CurrentFileOffset=-1
fileCompleteness = list()
gotTheCheksum = False
currentFileName = "HH"
checksum = 0
currentCID = 0
dst_addr = ""
file_name = "test.png"
port = 8888
home_port = 1111
p = None
q = None
test=0
normalHandshake = True
files_done = list()

v = False
show_write = False
show_recv = False
show_all = False
logger = None
show_nacks = False
js = dict()

simulationState = sState.nNotLost


def simulationSend(client_socket, packet):
    global q, p, simulationState
    try:
        if simulationState == sState.nLost:
            if random() <= q:
                pass
            else:
                # print(packet)
                client_socket.sendto(bytes(packet), (dst_addr, port))
                simulationState = sState.nNotLost
                if(not v):
                    logger.debug(packet.simple_str())            
                else:
                    logger.debug(packet)
        elif simulationState == sState.nNotLost:
            if random() <= p:
                simulationState = sState.nLost
            else:
                # print(packet)
                client_socket.sendto(bytes(packet), (dst_addr, port))
                if(not v):
                    logger.debug(packet.simple_str())            
                else:
                    logger.debug(packet)
    except IOError as e:
        logger.debug("Server not reachable")
        pass #In case of no network connection, Once the connectin is back the client should be
             #able to send again and the server can send to the correct client again
    

def bound_socket(source_ip, port, *a, **k):
    sock = socket.socket(*a, **k)
    if socket.AF_INET6 in a:
        if not socket.has_ipv6:
            raise ValueError("There's no support for IPV6!")
        else:
            address = [addr for addr in socket.getaddrinfo(source_ip, None)
                       if socket.AF_INET6 == addr[0]]  # You ussually want the first one.
            if not address:
                raise ValueError("Couldn't find ipv6 address for source %s" % source_ip)
            sock.bind(address[0][-1])
    else:
        sock.bind((source_ip, port))
    return sock


def resumeOrDone(file, needAlarm):
    global js
    if os.path.isfile("./" + file):
        if js.get(file,None) is not None:
            file_info = js[file]
            return True, False, file_info[2] , bytes.fromhex(file_info[0])
        else:
            if needAlarm:
                logger.info("The desired file "+ file + " has already been downloaded before!")
            return False,True, -1, b''
    return False,False,0,b''



def resumption_handshake(socket_poll, client_socket, file_name, givenFileOffset, checkSum, tries=3, timeout=1000):
    handshake_not_done = True
    res_client_packet = rft_packet.rft_packet.create_client_resumption_handshake(file_name, givenFileOffset, checkSum)
    while tries > 0 and handshake_not_done:
        # Creating the first handshake packet

        # client_socket.sendto(bytes(res_client_packet), (rft.dst_addr, port))
        simulationSend(client_socket, res_client_packet)
        # Wait for response from the server (wait timeout for each iteration)
        event_list = socket_poll.poll(timeout)
        for fd, event in event_list:
            if fd == client_socket.fileno():
                data, src = client_socket.recvfrom(1500)
                server_response = rft_packet.rft_packet(data)

                if v:
                    logger.debug(server_response)
                else:
                    logger.debug(server_response.simple_str())
                if server_response.isStc():
                    if (not server_response.isAck()):
                        sc, mes = server_response.get_status_code()
                        if sc == rft_packet.rft_status_codes.File_changed:
                            code, msg = server_response.get_status_code()
                            logger.info("Trying from the beginning: Status code {0} received: {1}".format(code, msg))
                            os.remove(file_name)  # get rid of wrong file
                            return exchange_handshake(socket_poll, client_socket,
                                                      file_name)  # soft reset from client by starting over the handshake

                        elif sc == rft_packet.rft_status_codes.File_not_available:
                            code, msg = server_response.get_status_code()
                            logger.info("Status code {0} received: {1}".format(code, msg))
                            return (False, None, None, None)

                        elif sc == rft_packet.rft_status_codes.Connection_Terminated:  # Other side MUST ack that reset / error
                            client_response = rft_packet.rft_packet.create_status_ack_packet(server_response.getCID(),
                                                                                             server_response.file_offset,
                                                                                             server_response.get_status_code())
                            # client_socket.sendto(bytes(client_response), (dst_addr, port))
                            simulationSend(client_socket, client_response)
                        elif sc == rft_packet.rft_status_codes.Version_mismatch or sc == rft_packet.rft_status_codes.Timeout or sc == rft_packet.rft_status_codes.Unknown:  # connection termination Required
                            code, msg = server_response.get_status_code()
                            logger.error("Status code {0} received: {1}".format(code, msg))
                            exit(0)
                        else:  # any status code
                            logger.error("Status code is NOT known")
                            exit(0)
                    else:
                        logger.debug("ACK received for the Status message with the status code {0}",
                              server_response.get_status_code()[0])

                if server_response.isRes():
                    logger.info("CID: {0}".format(server_response.getCID()))
                    client_response = rft_packet.rft_packet.create_client_resumption_ack(server_response.getCID(),
                                                                                         server_response.getFileoffset())
                    logger.info("Send Client ACK")
                    # client_socket.sendto(bytes(client_response), (dst_addr, port))
                    simulationSend(client_socket, client_response)
                    handshake_not_done = False
                    # Handshake done
                    return True, client_response.getCID(), server_response.checksum, client_response
        tries -= 1

    return False, None, None, None


# Deal with the inital Handshake #TODO: Handel Status messages DONE
def exchange_handshake(socket_poll, client_socket, file_name, tries=3, timeout=1000):
    handshake_not_done = True
    global normalHandshake
    normalHandshake = True
    first_packet = rft_packet.rft_packet.create_client_handshake(file_name)
    while (tries > 0 and handshake_not_done):
        # Creating the first handshake packet

        # client_socket.sendto(bytes(first_packet), (dst_addr, port))
        simulationSend(client_socket, first_packet)
        # Wait for response from the server (wait timeout for each iteration)
        event_list = socket_poll.poll(timeout)
        for fd, event in event_list:
            if (fd == client_socket.fileno()):
                data, src = client_socket.recvfrom(1500)
                server_response = rft_packet.rft_packet(data)
                logger.debug("Server Packet:")
                if v:
                    logger.debug(server_response)
                else:
                    logger.debug(server_response.simple_str())

                if server_response.isStc():
                    #every packet in hand shake is a problem
                    if (not server_response.isAck()):
                        sc, mes = server_response.get_status_code()
                        if sc == rft_packet.rft_status_codes.File_changed:
                            logger.info("Status code {0} received: {1} \n Trying from the beginning".format(sc, mes))
                            os.remove(file_name)  # get rid of wrong file
                            break  # soft reset from client by starting over the handshake in next try

                        elif sc == rft_packet.rft_status_codes.File_not_available:

                            logger.info("Status code {0} received: {1}".format(sc, mes))
                            return (False, None, None, None)

                        elif sc == rft_packet.rft_status_codes.Connection_Terminated:  # Other side MUST ack that reset / error
                            client_response = rft_packet.rft_packet.create_status_ack_packet(server_response.getCID(),
                                                                                             server_response.file_offset,
                                                                                             server_response.get_status_code())
                            # client_socket.sendto(bytes(client_response), (dst_addr, port))
                            simulationSend(client_socket, client_response)

                        elif sc == rft_packet.rft_status_codes.Version_mismatch or sc == rft_packet.rft_status_codes.Timeout or sc == rft_packet.rft_status_codes.Unknown:  # connection termination Required
                            logger.error("Status code {0} received: {1}".format(sc, mes))
                            exit(0)
                        else:  # any status code
                            logger.error("Status code is NOT known")
                            exit(0)
                    else:
                        logger.debug("ACK received for the Status message with the status code {0}",
                              server_response.get_status_code()[0])

                if (server_response.isNew()):
                    client_response = rft_packet.rft_packet.create_client_hello_ack(server_response.getCID())
                    # client_socket.sendto(bytes(client_response), (dst_addr, port))
                    simulationSend(client_socket, client_response)
                    handshake_not_done = False
                    # Handshake done
                    return (True, client_response.getCID(), server_response.payload, client_response)
        tries -= 1

    return (False, None, None, None)


def takeOutTheNotCompletedInfo(file):
    global js
    if os.path.isfile("./" + file):
        if js.get(file,None) is not None:
            js.pop(file)
    else:
        logger.debug("Nothing to remove")



def clientCaller(host, dst_port, pGiven, qGiven,
                 files):  # parser_result.host, parser_result.t port, parser_result.p, parser_result.q, parser_result.files
    global dst_addr,port
    port = dst_port
    datathread.logger = logger
    datathread.show_write = show_write
    datathread.show_nacks = show_nacks
    # different default for client
    #if port == 8888:
    #    port = 8889
    IPv4 = True
    # Check for input address IPv4/IPv6
    try:
        socket.inet_pton(socket.AF_INET, host)
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6, host)
        except socket.error:
            logger.critical("Invalid Address")
            return
        # Given address is IPv6
        IPv4 = False

    # Create socket for the given address type
    local_port = 1111
    if (IPv4):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_socket.bind(("", local_port))
    else:
        client_socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        client_socket.bind(("", local_port))
    dst_addr = host
    socket_poll = select.poll()
    socket_poll.register(client_socket, select.POLLIN)
    global p, q
    p = pGiven
    q = qGiven
    client(socket_poll, client_socket, files)


def fileRequest(file, socket_poll, client_socket):
    global fileCompleteness
    fileCompleteness.append(True)
    global currentFileName
    currentFileName = file
    notFoundInLog = True
    global normalHandshake
    file_ok = False  # default start
    global checksum, currentCID
    global js

    if os.path.isfile("./" + file):
        # read json file for (filename , checksum, currentCID) 
        file_info = js.get(file,None)
        if file_info is None:
            logger.info("This file should be already downloaded")
            return False, False, None, None, None
        
        #res request
        else:
            normalHandshake = False
            file_ok, currentCID, checksum, last_handshake_packet = resumption_handshake(socket_poll, client_socket,
                                                                                        file, file_info[2],
                                                                                        bytes.fromhex(file_info[0]))
            if(normalHandshake):
                normalHandshake = False
                return file_ok, False, last_handshake_packet, js, 0
            handshake_done = True
            logger.debug('Handshake "completed" by resumption')
            return file_ok, handshake_done, last_handshake_packet, js, file_info[2]

    #new request
    else:
        file_ok, currentCID, checksum, last_handshake_packet = exchange_handshake(socket_poll, client_socket, file)
        logger.debug('Handshake "completed"')
        handshake_done = False
        return file_ok, handshake_done, last_handshake_packet, None, 0
    

def grace_exithandler(socket):
    # Handle gracefully terminate
    # Server will stop sending packet when there's no ACK sent to server. c---STC =  Connection Terminated ---> s
    #print('SIGINT or CTRL-C detected. Exiting gracefully')
    termination_packet = rft_packet.rft_packet.create_status(0 if (currentCID is None) else currentCID, rft_packet.rft_status_codes.Connection_Terminated,
                                                             rft_packet.STC, "gracefull Termination")
    # socket.sendto(bytes(termination_packet), (dst_addr, port))
    simulationSend(socket, termination_packet)
    exit(0)

#main thread for client to handle packets
def client(socket_poll, client_socket, files):
    # file_name = files[0]
    global checksum
    global gotTheCheksum
    global test
    global CurrentFileOffset
    global fileCompleteness
    global currentFileName
    global js
    passedTime = datetime.now()
    # signal(SIGINT,partial(grace_exithandler,client_socket,currentCID))
    atexit.register(grace_exithandler,client_socket)
    i = 0
    max_ack = 0
    handshake_done = False
    while i < len(files):
        file_name = files[i]
        logger.info("Trying file: {0}".format(file_name))
        currentFileName = file_name
        if i == 0 and handshake_done == False:

            file_ok, handshake_done, last_handshake_packet, jsonObj, fileOffset = fileRequest(file_name, socket_poll,
                                                                                              client_socket)
            if( not file_ok):
                files.pop(0)
                fileCompleteness.pop(0)
                continue
            else:
                logger.debug("Valid first Handshake")
            if(fileOffset==0) or normalHandshake == False:
                gotTheCheksum = True
        else:
            file_ok = True
            handshake_done = True

            logger.debug("{0} th file from  files length: {1}".format(i + 1, len(files)))
            # there are more files to send so  FIN|ACK|NEW, length=fn_length, CID=oldCID, FO=endoffile | filename
            resume, done, lastOffset, nextChecksum = resumeOrDone(file_name, True)
            logger.debug("Last Offset got from resumption {0}".format(lastOffset))
            CurrentFileOffset = lastOffset
            if resume:
                create_client_endOfFileResume = rft_packet.rft_packet.create_client_endOfFileResumeRequest_ack(
                    packet.getCID(), lastOffset, nextChecksum,
                    file_name)
                # client_socket.sendto(bytes(create_client_endOfFileResume), (dst_addr, port))
                simulationSend(client_socket, create_client_endOfFileResume)
                passedTime = datetime.now()
                gotTheCheksum = False
                last_handshake_packet = create_client_endOfFileResume
                fileOffset = lastOffset
            elif not done:
                create_client_endOfFilesNew = rft_packet.rft_packet.create_client_endOfFileNewRequest_ack(
                    0 if (currentCID is None) else currentCID, offset=max_ack,
                    filename=file_name)
                # client_socket.sendto(bytes(create_client_endOfFilesNew), (dst_addr, port))
                simulationSend(client_socket, create_client_endOfFilesNew)
                passedTime = datetime.now()
                gotTheCheksum = False
                last_handshake_packet = create_client_endOfFilesNew
                fileOffset = lastOffset
            else:
                logger.info("The desired file " + file_name + " has already been fully downloaded before!")
                if i < len(fileCompleteness):
                    logger.debug("Trying the next file or terminate")
                    i += 1
                    if i < len(files):
                        fileCompleteness.append(False)
                    continue
        if i >= len(fileCompleteness):
            logger.warning("No connection was possible")
            return
        fileCompleteness[i] = False
        retry = False
        # Go into the transfer of the file
        # TODO: flow control use (send dtr, maybe use queue to determine the new data rate),
        if (file_ok):
            lastAckTimestamp = datetime.now()
            file = open(file_name, "ab+")  # append, binary write ? used to be x w
            # file = open(file_name, "wb")
            # print(fileOffset)
            #exit(0)
            write_queue = datathread.data_write_queue(file, fileOffset)
            logger.info("Starting from fileoffset: {0}".format(fileOffset))
            finReceived = False
            max_ack = 0
            packets_received = 0

            while (True):
                if(retry):
                    retry = False
                    #print(i)
                    break
                if fileCompleteness[i]:
                    #print("SORRY")
                    i = i + 1
                    if i < len(files):
                        fileCompleteness.append(False)
                    break
                # time.sleep(1)
                event_list = socket_poll.poll(0)
                for fd, event in event_list:

                    if fd == client_socket.fileno():

                        data, src = client_socket.recvfrom(1500)
                        packet = rft_packet.rft_packet(data)

                        if(show_recv):
                            logger.debug("Received Packet:")
                            if v:
                                logger.debug(packet)
                            else:
                                logger.debug(packet.simple_str())
                        # print("Server:")
                        # print(packet)
                        #if packets_received == 5:  # TESTING ONLY
                        #   exit()
                        # Handshake completion in case the last ACK is lost
                        if not handshake_done:
                            if (packet.isNew()) or packet.isRes():
                                # client_socket.sendto(bytes(last_handshake_packet), (dst_addr, port))
                                logger.debug("Trying to complete Handshake")
                                simulationSend(client_socket, last_handshake_packet)
                                continue
                            else:
                                handshake_done = True
                                gotTheCheksum = True
                                resume, done, unnecessary, unnecessary2 = resumeOrDone(file_name, False)
                                if resume:
                                    takeOutTheNotCompletedInfo(file_name)
                                fileCompleteness[i] = False

                        if handshake_done and not gotTheCheksum and i != 0:
                            if packet.isJustDataAck():
                                if 2< (datetime.now()-passedTime).total_seconds():
                                    passedTime =datetime.now()
                                    if v:
                                        logger.debug("Resending {0}".format(last_handshake_packet))
                                    else:
                                        logger.debug("Resending {0}".format(last_handshake_packet.simple_str()))
                                    # client_socket.sendto(bytes(last_handshake_packet), (dst_addr, port))
                                    simulationSend(client_socket, last_handshake_packet)
                                continue
                        if packet.isRes() or packet.isNew():  # handshake done but server sends NEW or RES
                            if gotTheCheksum:
                                msg = "Received a New connection packet while data packet expected"
                                status_packet = rft_packet.rft_packet.create_status(currentCID,
                                                                                    rft_packet.rft_status_codes.Unknown,
                                                                                    rft_packet.STC, msg)
                                # client_socket.sendto(bytes(status_packet), (dst_addr, port))
                                simulationSend(client_socket, status_packet)
                            else:
                                gotTheCheksum = True
                                if packet.payload is None:  # Resume
                                    checksum = packet.checksum
                                    takeOutTheNotCompletedInfo(file_name)
                                else:
                                    checksum = packet.payload  # New

                                continue
                        if packet.isStc():
                            if not packet.isAck():
                                sc, msg = packet.get_status_code()
                                if sc == rft_packet.rft_status_codes.File_not_available or sc == rft_packet.rft_status_codes.Version_mismatch or sc == rft_packet.rft_status_codes.Timeout or sc == rft_packet.rft_status_codes.Unknown:  # connection termination Required
                                    logger.info("Status code {0} received: {1}".format(sc, msg))
                                    fileCompleteness[i]=True #SET COMPLete so that you can get to the second file
                                    file.close()
                                    if os.path.isfile("./" + file_name):
                                        os.remove(file_name)
                                elif sc == rft_packet.rft_status_codes.File_changed: #Retry for file 
                                    file_ok = False
                                    continue

                                elif sc == rft_packet.rft_status_codes.Connection_Terminated:  # Other side MUST ack that reset / error
                                    client_response = rft_packet.rft_packet.create_status_ack_packet(
                                        packet.getCID(), packet.file_offset,
                                        packet.get_status_code())
                                    # client_socket.sendto(bytes(client_response), (dst_addr, port))
                                    simulationSend(client_socket, client_response)
                                else:  # any status code
                                    logger.critical("Status code is NOT known")
                                    exit(0)
                            else:
                                logger.info("ACK received for the Status message with the status code {0}",
                                      packet.get_status_code()[0])

                        if( not gotTheCheksum):
                            logger.debug("Did not receive Checksum yet")
                            continue
                        
                        write_queue.add(packet)

                        packets_received += 1

                        #?also ack the stc payload length
                        if(packet.payload is not None):
                            max_ack = max(packet.getFileoffset() + len(packet.payload), max_ack)
                            
                        # TODO: Remove for testing only
                        # print(" data frame received :\n {0}".format(packet.payload))


                        CurrentFileOffset =write_queue.write()

                        if packet.isFin():
                            finReceived = True

                        # if packet.isAck():
                        #    print("Received data packet with {0} file offset".format(packet.file_offset))
                        # last data packet from the server and ALL other has arrived
                        # print(write_queue.get_missing_ranges())
                        if finReceived and packet.isAck() and (write_queue.get_missing_ranges()[2] == b''):
                            CurrentFileOffset = write_queue.write()
                            file.close()

                            if(js.get(file_name,None) is not None):
                                js.pop(file_name)
                            # print("her")
                            calculatedChecksum = hashlib.sha256(open(file_name, "rb").read()).digest()
                            if calculatedChecksum == checksum:
                                logger.info("Checksum matches, file {0} fully downloaded.".format(file_name))
                                fileCompleteness[i] = True
                                #Send a final ack
                                packet = rft_packet.rft_packet.create_ack_packet(currentCID, max_ack, rft_packet.ACK,
                                                                                 nack_ranges=write_queue.get_missing_ranges()[2])
                                simulationSend(client_socket, packet)
                                if i + 1 == len(files):  # last file Requested
                                    # end connection ACK | FIN | STC, length = 0, CID = est, FO = n
                                    logger.info("Done with {0} of {1}".format(i + 1, len(files)))
                                    client_endOfFiles_ack = rft_packet.rft_packet.create_client_endOfFiles_ack(
                                        currentCID, max_ack)
                                    # client_socket.sendto(bytes(client_endOfFiles_ack), (dst_addr, port))
                                    simulationSend(client_socket, client_endOfFiles_ack)
                                    #Unregister 
                                    atexit.unregister(grace_exithandler)
                                    if v:
                                        logger.debug(client_endOfFiles_ack)
                                    else:
                                        logger.debug(client_endOfFiles_ack.simple_str())
                                    return

                            else:  # checksum not matched file error must be send #### CLIENT does not send File changed
                                #status_packet = rft_packet.rft_packet.create_status(currentCID,
                                #                                                    rft_packet.rft_status_codes.File_changed,
                                #                                                    rft_packet.STC,
                                #                                                    "Checksum mismatch")
                                # client_socket.sendto(bytes(status_packet), (dst_addr, port))
                                #simulationSend(client_socket, status_packet)
                                # FOR SOFT reset client need to ask again the same file #TODO What happens if i is 0
                                logger.warning("Checksum of retrieve file does not match the expectation! Trying again.")
                                retry = True
                                if os.path.isfile("./"+file_name):
                                    os.remove(file_name)
                                

                currentTime = datetime.now()
                if packets_received > 29 or lastAckTimestamp < (currentTime - timedelta(seconds=1)):
                # if packets_received > 9 :
                    logger.debug('Sending ACK {0}'.format(packets_received))
                    nack_range_size_ok, last_viable_offset, nack_ranges = write_queue.get_missing_ranges()
                    if(nack_range_size_ok):
                        packet = rft_packet.rft_packet.create_ack_packet(currentCID, max_ack, rft_packet.ACK,
                                                                     nack_ranges=nack_ranges)
                    else:
                        packet = rft_packet.rft_packet.create_ack_packet(currentCID, last_viable_offset,rft_packet.ACK, nack_ranges=nack_ranges)
                        logger.debug("Missing ranges out of range")

                    #print("ACK {0}".format(write_queue))
                    # client_socket.sendto(bytes(packet), (dst_addr, port))
                    simulationSend(client_socket, packet)
                    packets_received = 0
                    lastAckTimestamp = currentTime
                    if((not gotTheCheksum )and handshake_done):
                        passedTime =datetime.now()
                        if v:
                            logger.debug("Timeout: Resending {0}".format(last_handshake_packet))
                        else:
                            logger.debug("Timeout: Resending {0}".format(last_handshake_packet.simple_str()))
                        # client_socket.sendto(bytes(last_handshake_packet), (dst_addr, port))
                        simulationSend(client_socket, last_handshake_packet)

        else:
            if len(files) > files.index(files[i]) + 1:
                logger.info("Continue with the next file")
        # i = i + 1

# client()
