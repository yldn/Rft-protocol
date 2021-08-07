import rft_packet
import rft_server
import rft_client

import argparse



parser = argparse.ArgumentParser(description="RFT-Implementation")
parser.add_argument("host",metavar="::",default="::",nargs="?")
parser.add_argument("-s", action="store_true")
parser.add_argument("-t",metavar="1111",type=int,default=8888)
parser.add_argument("-p",metavar="0.5",type=float,default=0)
parser.add_argument("-q",metavar="0.5",type=float,default=0)
parser.add_argument("files",metavar="test.txt",default="[test.txt]",nargs="*")


parser_result = parser.parse_args()

if(parser_result.p!=0 and parser_result.q==0):
    parser_result.q=parser_result.p
elif(parser_result.q!=0 and parser_result.p==0):
    parser_result.p=parser_result.q
# init client
if(not parser_result.s):
    rft_client.client(parser_result.host,parser_result.t,parser_result.p,parser_result.q,parser_result.files)
# init server
else:
    rft_server.server(parser_result.t,parser_result.p,parser_result.q)









