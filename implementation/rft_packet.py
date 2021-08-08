

ACK = 128
NEW = 64
FIN = 32
STC = 16
DTR = 8
RES = 4

timeout_time = 1

class rft_status_codes:
    Unknown = 0
    Connection_Terminated = 1
    File_not_available = 2
    File_changed = 3
    Version_mismatch = 4
    Timeout = 5

class rft_packet:
    
    def __init__(self,udp_payload):
        self.version = udp_payload[0:1]
        self.flags = int.from_bytes(udp_payload[1:2],byteorder="big")
        self.length = udp_payload[2:4]
        self.cid = udp_payload[4:8] 
        self.file_offset = udp_payload[8:16]
        self.dtr = None
        if(self.flags&DTR == DTR):
            #DTR is set:
            self.dtr = int.from_bytes(udp_payload[16:24],byteorder="big")
            #check stc & message
            if(self.flags&STC == STC):
                self.stc = udp_payload[24:25]
                self.msg = udp_payload[25:].decode("UTF-8")
            else:
                self.payload = udp_payload[24:]
        else:
            self.dtr = None
            if(self.flags&STC == STC):
                self.stc = udp_payload[16:17]
                self.msg = udp_payload[17:].decode("UTF-8")
            self.payload = udp_payload[16:]
        
    @staticmethod
    def create_resumption_packet(payload,cid,fileoffset):
        packet = rft_packet(b'')
        if type(payload) is bytes : 
            packet.payload = payload
        else :
            packet.payload =  payload.encode("UTF-8")
        
        packet.version = b'\x01'
        packet.flags = RES
        packet.length = (len(packet.payload)).to_bytes(2,byteorder="big")
        packet.cid = (cid).to_bytes(4,byteorder="big")
        packet.file_offset = (fileoffset).to_bytes(8,byteorder="big")
        return packet


    @staticmethod
    def create_client_handshake(filename):
        packet = rft_packet(b'')
        packet.payload = filename.encode("utf-8")
        packet.version = b'\x01'
        packet.flags = NEW
        packet.length = (len(packet.payload)).to_bytes(2,byteorder="big")
        packet.cid = (0).to_bytes(4,byteorder="big")
        packet.file_offset = (0).to_bytes(8,byteorder="big")
        return packet
        

    @staticmethod
    def create_server_handshake(cid,payload):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = NEW
        packet.length = len(payload).to_bytes(2,byteorder="big")
        packet.cid = cid.to_bytes(4,byteorder="big")
        packet.file_offset = (0).to_bytes(8,byteorder="big")
        packet.payload = payload
        print(int.from_bytes(packet.length,byteorder="little"),len(payload))
        return packet
        

    @staticmethod
    def create_client_hello_ack(cid,dtr):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = NEW | ACK
        packet.length = (0).to_bytes(2,byteorder="big")
        packet.cid = cid.to_bytes(4,byteorder="big")
        packet.file_offset = (0).to_bytes(8,byteorder="big")
        packet.payload = b''
        if( dtr is not None):
            packet.flags = packet.flags | DTR
            packet.dtr = dtr.to_bytes(8,byteorder="big")
        return packet

    @staticmethod
    def create_status(cid,status_code,flags = STC,message=""):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | STC
        packet.stc = status_code.to_bytes(1, byteorder="big")
        packet.msg = message
        packet.payload = status_code.to_bytes(1,byteorder="big") + message.encode("utf-8")
        packet.length = len(packet.payload).to_bytes(2,byteorder="big")
        packet.cid = cid.to_bytes(4,byteorder="big")
        packet.file_offset = (0).to_bytes(8,byteorder="big")
        return packet

    @staticmethod
    def create_data_packet(cid,data,file_offset, flags = ACK):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | ACK
        packet.payload = data
        packet.length = len(packet.payload).to_bytes(2,byteorder="big")
        packet.cid = cid.to_bytes(4,byteorder="big")
        packet.file_offset = (file_offset).to_bytes(8,byteorder="big")
        return packet

    @staticmethod
    def create_ack_packet(cid,file_offset,flags = ACK,nack_ranges=b'',dtr=None):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | ACK 
        packet.length = len(nack_ranges).to_bytes(2,byteorder="big")
        packet.payload = nack_ranges
        packet.cid = cid.to_bytes(4,byteorder="big")
        packet.file_offset = (file_offset).to_bytes(8,byteorder="big")
        if( dtr is not None):
            packet.flags = packet.flags | DTR
            packet.dtr = dtr.to_bytes(8,byteorder="big")
        return packet


    def create_fin_packet(args):
        pass
        

    def getCID(self):
        return int.from_bytes(self.cid,"big",signed=False)

    def getFileoffset(self):
        return int.from_bytes(self.file_offset,"big",signed=False)

    def get_nack_ranges(self):
        length = int.from_bytes(self.length,byteorder="big")
        if(int.from_bytes(self.length,byteorder="big")<16):
            return []
        start = 0
        nack_ranges_list = list()
        while(True):
            length -= 16
            if(length<=0):
                return nack_ranges_list
            nack_ranges_list.append((int.from_bytes(self.payload[start:8],"big",signed=False),int.from_bytes(self.payload[start+8:start+16],"big",signed=False)))
            start += 16
        return nack_ranges_list


    def isAck(self):
        return True if self.flags&ACK == ACK else False

    def isNew(self):
        return True if self.flags&NEW == NEW else False

    def isFin(self):
        return True if self.flags&FIN == FIN else False

    def isStc(self):
        return True if self.flags&STC == STC else False

    def isDtr(self):
        return True if self.flags&DTR == DTR else False

    def isRes(self):
        return True if self.flags&RES == RES else False

    def getlength(self):
        return int.from_bytes(self.length,"big",signed=False)

    def get_status_code(self):
        if(self.isStc()):
            return self.stc
        else:
            return None

    def get_status_msg(self):
        if(self.isStc()):
            return self.msg
        else:
            return None

    def __bytes__(self):
        if(self.isDtr()):
            return (self.version + self.flags.to_bytes(1,byteorder="little") + self.length + self.cid + self.file_offset + self.dtr + self.payload)
        else:
            return (self.version + self.flags.to_bytes(1,byteorder="little") + self.length + self.cid + self.file_offset + self.payload)

    def __str__(self):
        str = "RFT Packet Information:\n"
        str += "     Version: {0}\n".format(int.from_bytes(self.version,byteorder="big"))
        str += "     Flags: "
        if(self.isAck()):
            str += "ACK "
        if(self.isNew()):
            str += "NEW "
        if(self.isFin()):
            str += "FIN "
        if(self.isDtr()):
            str += "DTR "
        if(self.isStc()):
            str += "STC "
        if(self.isRes()):
            str += "RES "
        str += "\n     Length: {0}\n".format(int.from_bytes(self.length,byteorder="big"))
        str += "     CID: {0}\n".format(int.from_bytes(self.cid,byteorder="big"))
        str += "     FO: {0}\n".format(self.getFileoffset())
        if(self.isDtr()):
            str += "     Datarate: {0}\n".format(self.dtr)
        str += "     Payload: {0}".format(self.payload)
        return str

