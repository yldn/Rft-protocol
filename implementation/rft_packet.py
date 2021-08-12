import hashlib

MAXFILENAME = 1500
CHECKSUMLENGTH = 32  # len checksum is  256 bit = 32 byte
MAXmessage = 1500
MAXDATA = 2500
MAXMissingSEGMENT = 1000

ACK = 128
NEW = 64
FIN = 32
STC = 8
DTR = 16
RES = 4



timeout_time = 5
payload_size = 512 #MAX 1484 

class rft_status_codes:
    Unknown = 0
    Connection_Terminated = 1
    File_not_available = 2
    File_changed = 3
    Version_mismatch = 4
    Timeout = 5


class rft_packet:
    m = hashlib.sha256()

    def __init__(self, udp_payload):
        self.version = udp_payload[0:1]
        self.flags = int.from_bytes(udp_payload[1:2], byteorder="big")
        self.length = udp_payload[2:4]
        self.cid = udp_payload[4:8]
        self.file_offset = udp_payload[8:16]
        self.filename = None
        self.checksum = None
        self.payload = None
        dtr_offset = 0
        if (self.flags & DTR == DTR):
            # DTR is set:
            dtr_offset = 8
            self.dtr = int.from_bytes(udp_payload[16:24], byteorder="big")  # todo: RFC says that DTR is after NACKs
        else:
            self.dtr = None

        
        if(self.flags & STC == STC):
            self.stc = udp_payload[16+dtr_offset:17+dtr_offset]
            self.msg = udp_payload[17+dtr_offset:].decode("UTF-8")
        
        elif(self.flags & RES == RES):
            self.checksum = udp_payload[16+dtr_offset:16+dtr_offset+CHECKSUMLENGTH]
            if len(udp_payload) > 16 + CHECKSUMLENGTH:
                self.filename = udp_payload[16+dtr_offset+CHECKSUMLENGTH:]
            self.payload = udp_payload[16+dtr_offset:]
        else:
            self.payload = udp_payload[16+dtr_offset:]


    @staticmethod
    def create_client_handshake(filename):
        packet = rft_packet(b'')
        packet.payload = filename.encode("utf-8")
        packet.version = b'\x01'
        packet.flags = NEW
        packet.length = (len(packet.payload)).to_bytes(2, byteorder="big")
        packet.cid = (0).to_bytes(4, byteorder="big")
        packet.file_offset = (0).to_bytes(8, byteorder="big")
        return packet

    # might be unnesseaysry
    @staticmethod
    def create_client_endOfFile_ack(cid, offset):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = FIN | ACK | STC
        packet.length = (len(packet.payload)).to_bytes(2, byteorder="big")  # should be 0
        print("Lengh of finack: {0}".format(int.from_bytes(packet.length, byteorder="big")))
        packet.cid = (cid).to_bytes(4, byteorder="big")
        packet.file_offset = (offset).to_bytes(8, byteorder="big")
        return packet

    @staticmethod
    def create_client_endOfFileNewRequest_ack(cid, offset, filename):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = FIN | ACK | NEW
        packet.payload = filename.encode("utf-8")
        packet.filename = filename.encode("utf-8")
        packet.length = (len(packet.payload)).to_bytes(2, byteorder="big")
        packet.cid = (cid).to_bytes(4, byteorder="big")
        packet.file_offset = (offset).to_bytes(8, byteorder="big")
        return packet

    def create_client_endOfFileResumeRequest_ack(cid, middleoffset, checksum, filename):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = FIN | RES
        packet.checksum = checksum
        print("Payload length: ",len(checksum))
        packet.payload = checksum + filename.encode("utf-8")
        packet.length = (len(packet.payload)).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = (middleoffset).to_bytes(8, byteorder="big")
        return packet

    @staticmethod
    def create_client_resumption_handshake(filename, givenFileOffset, checkSum):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = RES
        packet.checksum = checkSum
        packet.filename = filename.encode("utf-8")
        #print(len(packet.checksum))
        packet.payload = checkSum + filename.encode("utf-8")
        #print(len(packet.payload))
        packet.length = (len(packet.filename) + len(packet.checksum)).to_bytes(2, byteorder="big")
        packet.cid = (0).to_bytes(4, byteorder="big")
        packet.file_offset = (givenFileOffset).to_bytes(8, byteorder="big")
        return packet

    @staticmethod
    def create_server_handshake(cid, payload):  # payload =checksum
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = NEW
        packet.length = len(payload).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = (0).to_bytes(8, byteorder="big")
        packet.payload = payload
        # print(int.from_bytes(packet.length, byteorder="little"), len(payload))
        return packet

    @staticmethod
    def create_server_resumption_handshake(cid, checksum, givenOffset):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = RES
        packet.checksum = checksum
        packet.length = len(checksum).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = givenOffset.to_bytes(8, byteorder="big")
        packet.payload = checksum
        # print(int.from_bytes(packet.length, byteorder="little"), len(checksum))
        return packet

    @staticmethod
    def create_client_hello_ack(cid):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = NEW | ACK
        packet.length = (0).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = (0).to_bytes(8, byteorder="big")
        packet.payload = b''
        return packet

    @staticmethod
    def create_client_resumption_ack(cid, givenFileOffset):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = RES | ACK
        packet.length = (0).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = givenFileOffset.to_bytes(8, byteorder="big")
        packet.payload = b''
        return packet

    # n ACK | FIN | STC, length = 0, CID = est, FO = n
    @staticmethod
    def create_client_endOfFiles_ack(currentCID, givenFileOffset):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = FIN | ACK | STC
        packet.length = (0).to_bytes(2, byteorder="big")
        packet.cid = currentCID.to_bytes(4, byteorder="big")
        packet.file_offset = givenFileOffset.to_bytes(8, byteorder="big")
        packet.stc = rft_status_codes.Connection_Terminated.to_bytes(1, byteorder="big")
        packet.msg = "All files have been received"
        packet.payload = rft_status_codes.Connection_Terminated.to_bytes(1, byteorder="big") + packet.msg.encode(
            "utf-8")
        packet.length = (len(packet.payload)-1).to_bytes(2, byteorder="big")
        return packet

    @staticmethod
    def create_status(cid, status_code, flags=STC, message=""):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | STC
        packet.stc = status_code.to_bytes(1, byteorder="big")
        packet.msg = message
        packet.payload = status_code.to_bytes(1, byteorder="big") + message.encode("utf-8")
        packet.length = (len(packet.payload)-1).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = (0).to_bytes(8, byteorder="big")
        return packet

    @staticmethod
    def create_data_packet(cid, data, file_offset, flags=ACK):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | ACK
        packet.payload = data
        packet.length = len(packet.payload).to_bytes(2, byteorder="big")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = file_offset.to_bytes(8, byteorder="big")

        return packet

    @staticmethod
    def create_ack_packet(cid, file_offset, flags=ACK, nack_ranges=b'', dtr=None):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | ACK
        packet.length = len(nack_ranges).to_bytes(2, byteorder="big")
        packet.payload = nack_ranges
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = file_offset.to_bytes(8, byteorder="big")
        if (dtr is not None):
            packet.flags = packet.flags | DTR
            packet.dtr = dtr.to_bytes(8, byteorder="big")  # self-dtr changed could break something
        return packet

    @staticmethod
    def create_status_ack_packet(cid, file_offset, receivedSTC, receivedMessage, flags=ACK):
        packet = rft_packet(b'')
        packet.version = b'\x01'
        packet.flags = flags | ACK | STC
        packet.stc = receivedSTC.to_bytes(1, byteorder="big")
        packet.msg = receivedMessage.encode("utf-8")
        packet.cid = cid.to_bytes(4, byteorder="big")
        packet.file_offset = (file_offset).to_bytes(8, byteorder="big")
        packet.payload = receivedSTC.to_bytes(1, byteorder="big") + receivedMessage.encode("utf-8")
        packet.length = (len(packet.payload)-1).to_bytes(2, byteorder="big")
        return packet

    def getCID(self):
        return int.from_bytes(self.cid, "big", signed=False)

    def getFileoffset(self):
        return int.from_bytes(self.file_offset, "big", signed=False)

    def get_nack_ranges(self):
        length = int.from_bytes(self.length, byteorder="big")
        if (int.from_bytes(self.length, byteorder="big") < 16):
            return []
        start = 0
        nack_ranges_list = list()
        while (True):
            length -= 16
            if (length < 0):
                return nack_ranges_list
            nack_ranges_list.append((int.from_bytes(self.payload[start:start+8], "big", signed=False),
                                     int.from_bytes(self.payload[start + 8:start + 16], "big", signed=False)))
            start += 16
        # return nack_ranges_list

    # TODO: payload length checks?
    @staticmethod
    def lengthCheck(packet, isSendByServer):
        # print(packet)
        if(packet is None):
            return False
        expectedPayload = 0
        noVariableLength = True
        if packet.flags & DTR == DTR:
            expectedPayload += 8 #todo: WRONG IF dtr is not included in the lenght
        if packet.flags & NEW == NEW and packet.cid == 0:
            expectedPayload += MAXFILENAME
            noVariableLength = False
        if packet.flags & FIN != FIN and packet.flags & NEW == NEW and packet.cid != 0 and packet.flags & ACK == ACK:
            expectedPayload += CHECKSUMLENGTH  # checksum
        if packet.flags & FIN == FIN and packet.cid != 0 and packet.flags & ACK == ACK and packet.flags & NEW == NEW:
            expectedPayload += MAXFILENAME
            noVariableLength = False
        if packet.flags & FIN == FIN and packet.cid != 0 and packet.flags & RES == RES:
            expectedPayload += CHECKSUMLENGTH + MAXFILENAME
            noVariableLength = False
        if packet.flags & FIN != FIN and packet.flags & RES == RES and packet.cid != 0:
            expectedPayload += CHECKSUMLENGTH  # checksum
        if packet.flags & RES == RES and packet.cid == 0:
            expectedPayload += CHECKSUMLENGTH + MAXFILENAME  # checksum
            noVariableLength = False
        if packet.flags & ACK == ACK and (
                not (packet.flags & RES == RES or packet.flags & NEW == NEW or packet.flags & STC == STC)):
            if isSendByServer:
                expectedPayload += MAXDATA
                noVariableLength = False
            else:
                expectedPayload += MAXMissingSEGMENT * 16
                noVariableLength = False
        if packet.flags & STC == STC and packet.flags & ACK != ACK:
            expectedPayload += MAXmessage

        return packet.length == len(packet.payload) and (
            packet.payload == expectedPayload if noVariableLength else packet.payload < expectedPayload)

    def isJustDataAck(self):
        return True if self.flags | ACK == ACK else False

    def isAck(self):
        return True if self.flags & ACK == ACK else False

    def isNew(self):
        return True if self.flags & NEW == NEW else False

    def isFin(self):
        return True if self.flags & FIN == FIN else False

    def isStc(self):
        return True if self.flags & STC == STC else False

    def isDtr(self):
        return True if self.flags & DTR == DTR else False

    def isRes(self):
        return True if self.flags & RES == RES else False

    def getlength(self):
        return int.from_bytes(self.length, "big", signed=False)

    def get_status_code(self):
        if (self.isStc()):
            return (int.from_bytes(self.stc, byteorder="big"), self.msg)
        else:
            return None

    def __bytes__(self):
        if (self.isDtr()):
            return (self.version + self.flags.to_bytes(1,
                                                       byteorder="little") + self.length + self.cid + self.file_offset + self.dtr + self.payload)
        else:

            return (self.version + self.flags.to_bytes(1,
                                                       byteorder="little") + self.length + self.cid + self.file_offset + self.payload)

    def __str__(self):
        str = "RFT Packet Information:\n"
        str += "     Version: {0}\n".format(int.from_bytes(self.version, byteorder="big"))
        str += "     Flags: "
        if (self.isAck()):
            str += "ACK "
        if (self.isNew()):
            str += "NEW "
        if (self.isFin()):
            str += "FIN "
        if (self.isDtr()):
            str += "DTR "
        if (self.isStc()):
            str += "STC "
        if (self.isRes()):
            str += "RES "
        str += "\n     Length: {0}\n".format(int.from_bytes(self.length, byteorder="big"))
        str += "     CID: {0}\n".format(int.from_bytes(self.cid, byteorder="big"))
        str += "     FO: {0}\n".format(int.from_bytes(self.file_offset, byteorder="big"))
        if self.flags & STC == STC:
            str += "     STC Code: {0} \t STC msg: {1}\n".format(int.from_bytes(self.stc, byteorder="big"), self.msg)
        if self.payload is None:
            str += "     Payload: -"
        else:
            str += "     Payload: {0}".format(self.payload)
        if self.checksum is not None:
            str += "     Checksum: {0}".format(self.checksum)
        if self.filename is not None:
            str += "     File Name: {0}".format(self.filename)

        return str

    def simple_str(self):
        str = "Packet: "
        str += "Flags: "
        if (self.isAck()):
            str += "ACK "
        if (self.isNew()):
            str += "NEW "
        if (self.isFin()):
            str += "FIN "
        if (self.isDtr()):
            str += "DTR "
        if (self.isStc()):
            str += "STC "
        if (self.isRes()):
            str += "RES "
        str += "Length: {0} ".format(int.from_bytes(self.length, byteorder="big"))
        str += "CID: {0} ".format(int.from_bytes(self.cid, byteorder="big"))
        str += "FO: {0}".format(int.from_bytes(self.file_offset, byteorder="big"))
        if(self.isDtr()):
            str += "DTR value: {0}".format(self.dtr if self.dtr is not None else 0)

        return str