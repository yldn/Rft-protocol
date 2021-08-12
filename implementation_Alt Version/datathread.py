import collections
import rft_packet
import threading

class data_packet_queue(threading.Thread):

    def __init__(self,cid,size,file,file_offset,buffersize):
        self.data_packets = collections.deque()
        self.size = size
        self.file = file
        self.buffersize = buffersize
        self.stop = False
        self.file_offset = file_offset
        self.cid = cid
        threading.Thread.__init__(self)
    def stop(self):
        self.stop = True

    # def append(packet):
    #     pass

    def add_missingpacket(self,packet):
        print('missing packet is readded tothe sending buffer: ',packet)
        self.data_packets.append(packet)   

    def run(self):
        while(not self.stop):
            #Does not really make it only use buffersize packts
            #Packts can get re-added because of loss, thus >buffersize
            if(len(self.data_packets)<self.buffersize):
                #read from current fileoffset and consturct a packet 
                self.file.seek(self.file_offset)
                new_data = self.file.read(self.size)

                #edge condition 
                if len(new_data) == 0:
                    print("NO MORE FRESH DATA LEFT TO READ")
                    self.stop = True
                    self.finalMaxFileOffset = self.file_offset

                # read last packet
                if(len(new_data)<self.size):
                    new_packet = rft_packet.rft_packet.create_data_packet(self.cid,new_data,self.file_offset,rft_packet.FIN)
                    self.file_offset += self.size
                    print('Loading finished... ',self.file_offset)
                    self.data_packets.appendleft(new_packet)
                    self.stop = True
                else:
                    new_packet = rft_packet.rft_packet.create_data_packet(self.cid,new_data,self.file_offset)
                    self.file_offset += self.size
                    print('data loading... ',self.file_offset)
                    #Add to the left side of the queue
                    self.data_packets.appendleft(new_packet)


class data_write_queue():
    
    def __init__(self, file,fileOffset):
        # receive buffer 
        self.queue = collections.deque()
        self.payload_dict = dict()
        self.file = file
        self.file_position = fileOffset  # Pointer to the position in the expected to be written next
        self.run = True
        self.fin = False
    def add(self, packet):
        self.queue.append(packet)

    def set_fin(self):
        self.file.flush()
        self.file.close()

    def stop(self):
        self.run = False
    def __str__(self):
        res= ""
        bytes_objj=  self.get_missing_ranges()
        L = [bytes_objj[i:i+16] for i in range(len(bytes_objj))]
        for k in L:
            res +=(str(int.from_bytes( k[0:8], byteorder="big"))+ " "+str( int.from_bytes( k[8:16], byteorder="big"))) +"\n"
        return res
    def get_missing_ranges(self):
        # print('get missing range :', self.payload_dict)
        key_values = list(self.payload_dict)
        if (len(key_values) == 0):
            return b''
        max_key_value = max(key_values)
        key_values.sort()
        ranges = list()
        start_pos = self.file_position

        for p in key_values:
            payload = self.payload_dict[p]
            if (start_pos == -1):
                start_pos = p + len(payload)
                continue
            if (p != start_pos):
                ranges.append((start_pos, p - 1))
                start_pos = p + len(payload)
                continue
            else:
                start_pos += len(payload)

        res = b''
        for r in ranges:
            res += r[0].to_bytes(8, byteorder="big") + r[1].to_bytes(8, byteorder="big")
        return res

    def corp_missing_ranges(self):
        missingrange = self.get_missing_ranges()
        if(len(missingrange)>16):
            res = missingrange[:8]+missingrange[-8:]
            print('nack payload:',res)
            return res
        else: return missingrange
        
        

    def write(self):
        # finish writing 
        if (len(self.queue) == 0 and self.fin):
            self.run = False
            return
        # load each received packet to dic 
        while (len(self.queue) > 0):
            # print('payload dict length:',len(self.payload_dict))
            packet = self.queue.popleft()
            pos = packet.getFileoffset()

            if (self.payload_dict.get(pos, None) is None):
                if(self.file_position<=pos):
                    self.payload_dict[pos] = packet.payload
        
        # print(self.payload_dict)
        # check if the request file_position has valid packet
        while(len(self.payload_dict)>0):
            res = self.payload_dict.pop(self.file_position, None)
            # a = list(self.payload_dict.keys())
            # a.sort()
            if(res is None):
                # requested packet is not received yet 
                break
            if (res is not None):
                self.file.write(res)
                # print("File position {0} written, new position {1}".format(self.file_position,self.file_position+len(res)))
                # print('write int to file: ',self.file_position)
                self.file_position += len(res)

        
        return self.file_position



