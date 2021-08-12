import os
from enum import Enum

import rft_server
import rft_client
import argparse
import json
import logging


parser = argparse.ArgumentParser(description="yaRFT-Implementation")
parser.add_argument("host",metavar="::1",default="::1",nargs="?")
parser.add_argument("-s", action="store_true",help="Start as Server")
parser.add_argument("-t",metavar="1111",type=int,default=8888,help="Port for Server/Client")
parser.add_argument("-p",metavar="0.5",type=float,default=0,help="p value for loss simulation")
parser.add_argument("-q",metavar="0.5",type=float,default=0,help="q value for loss simulation")
parser.add_argument("files",metavar="test.txt",default="[test.txt]",nargs="*")
parser.add_argument("-v", action="store_true",help="Show packet including payload in debug mode")
parser.add_argument("--write", action="store_true",help="Show write queue and write operations in debug mode")
parser.add_argument("--recv", action="store_true",help="Show received packet in debug mode")
parser.add_argument("--cwnd", action="store_true",help="Show information for the congestion window in debug mode")
parser.add_argument("--force", action ="store_true",help="Remove all requested files on the client side before starting the transfer")
parser.add_argument("--debug",action="store_true",help="Show debug information")
parser.add_argument("-ls", action="store_true",help="List files on server")
parser.add_argument("--missing",action="store_true",help="Show Missing Ranges in debug mode")
currentFileO=-1
delim=b'   '
parser_result = parser.parse_args()


if(parser_result.p!=0 and parser_result.q==0):
    parser_result.q=parser_result.p
elif(parser_result.q!=0 and parser_result.p==0):
    parser_result.p=parser_result.q

if(parser_result.v):
    rft_client.v = True
    rft_server.v = True
if(parser_result.cwnd):
    rft_server.show_cwnd = True

if(parser_result.write):
    rft_client.show_write = True

if(parser_result.recv):
    rft_client.show_recv = True
    rft_server.recv = True

if(parser_result.ls): 
    rft_server.listfiles = True

if(parser_result.missing):
    rft_server.show_nacks = True
    rft_client.show_nacks = True

if(not parser_result.s):
    if(parser_result.force):
        logging.info("Removing files")
        for f in parser_result.files:
            if(os.path.isfile("./"+f)):
                os.remove("./"+f)

    try:
        rft_client.logger = logging.getLogger("Client")
        rft_client.logger.propagate = False
        formatter = logging.Formatter("%(name)s:%(levelname)s: %(message)s")
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        rft_client.logger.addHandler(ch)
        if(parser_result.debug):
            rft_client.logger.setLevel(logging.DEBUG)
        else:
            rft_client.logger.setLevel(logging.INFO)

        fileList =list()
        for i in parser_result.files:
            if i not in fileList:
                fileList.append(i)
        #Create/Load Json
        if os.path.isfile("./clientlog.json"):
            jsonfile = open("clientlog.json")
            rft_client.js = json.load(jsonfile)
            jsonfile.close()

        else:
            rft_client.js = dict()
        
        rft_client.clientCaller(parser_result.host,parser_result.t,parser_result.p,parser_result.q,fileList)
    finally:
        rft_client.logger.info("Saving log")        
        
        #Add new file
        for b in rft_client.fileCompleteness:
            if not b and rft_client.CurrentFileOffset>0:
                rft_client.js[rft_client.currentFileName] = (rft_client.checksum.hex(),rft_client.currentCID,rft_client.CurrentFileOffset)
                break
        

        rft_client.logger.debug(rft_client.js)
        jsonfile = open("clientlog.json","w")
        json.dump(rft_client.js,jsonfile)
        jsonfile.close()
        


else:
    rft_server.logger = logging.getLogger("Server")
    formatter = logging.Formatter("%(name)s:%(levelname)s: %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    rft_server.logger.addHandler(ch)
    if(parser_result.debug):
        rft_server.logger.setLevel(logging.DEBUG)
    else:
        rft_server.logger.setLevel(logging.INFO)
    rft_server.server(parser_result.t,parser_result.p,parser_result.q)
    exit(0)









