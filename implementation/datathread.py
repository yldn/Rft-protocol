import collections
import rft_packet
import threading

class data_packet_queue(threading.Thread):

    def __init__(self,cid,size,file,buffersize):
        self.data_packets = collections.deque()
        self.size = size
        self.file = file
        self.buffersize = buffersize
        self.stop = False
        self.file_offset = 0
        self.cid = cid
        threading.Thread.__init__(self)
    def stop():
        self.stop = True

    def append(packet):
        pass
        

    def run(self):
        while(not self.stop):
            #Does not really make it only use buffersize packts
            #Packts can get re-added because of loss, thus >buffersize
            if(len(self.data_packets)<self.buffersize):
                #read from current file and consturct a packet 
                new_data = self.file.read(self.size)
                # read last packet
                if(len(new_data)<self.size):
                    self.stop = True
                    new_packet = rft_packet.rft_packet.create_data_packet(self.cid,new_data,self.file_offset,rft_packet.FIN)

                else:
                    #TODO: check edge-case filesize = size*x x {1,...g}
                    new_packet = rft_packet.rft_packet.create_data_packet(self.cid,new_data,self.file_offset)
                    self.file_offset += self.size

                #Add to the left side of the queue
                self.data_packets.appendleft(new_packet)



class data_write_queue():

    def __init__(self,file):
        self.queue = collections.deque()
        self.payload_dict = dict()
        self.file = file
        self.file_position = 0 #Pointer to the position in the expected to be written next
        self.run = True
        self.fin = False

    def add(self,packet):
        self.queue.append(packet)

    def set_fin(self):
        self.file.flush()
        self.file.close()
        
    def stop(self):
        self.run = False

    def get_missing_ranges(self):

        key_values = list(self.payload_dict)
        if(len(key_values)==0):
            return b''
        max_key_value = max(key_values)
        key_values.sort()
        ranges = list()
        start_pos = -1
        #TODO: fix missing ranges, missing ranges too far to the left
        for p in key_values:
            payload = self.payload_dict[p]
            if(start_pos == -1):
                start_pos = p + len(payload)
                continue
            if(p!=start_pos):
                ranges.append[(start_pos,p-1)]
                p = -1
        
        res = b''
        for r in range:
            res += r[0].to_bytes(8,byteorder="big") + r[1].to_bytes(8,byteorder="big")
        return res

    def write(self):
        if(len(self.queue)==0 and self.fin):
            self.run = False

        if(len(self.queue)>0):
            packet = self.queue.popleft()
            pos = packet.getFileoffset()
            if(self.payload_dict.get(self.file_position,None) is None):
                self.payload_dict[pos] = packet.payload

        
        
        res = self.payload_dict.pop(self.file_position,None)
        if(res is not None):
            self.file.write(res)
            self.file_position += len(res)


