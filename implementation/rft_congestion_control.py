import rft_packet


class rft_congestion_control:

    def __init__(self):
        # self.cwnd = 1
        # self.current_time = datetime.datetime.now().timestamp() 
        # self.udpate = False
        # self.current_cwnd
        # self.udpate_time
        pass

    def update_packet_loss(self):
        # if(not self.udpate):
        #     self.cwnd = self.cwnd >> 2
        #     if(self.cwnd == 0):
        #         self.cwnd = 1 
        #     self.update = True

        # if(self.current_time - datetime.datetime.now().timestamp() > self.udpate_time):
        #     self.current_time = datetime.datetime.now().timestamp()
        #     self.udpate = False
        pass
    def no_loss(self):
        # if(not self.udpate):
        #     self.cwnd += 5
        #     self.udpate = True

        # if(self.current_time - datetime.datetime.now().timestamp() > self.udpate_time):
        #     self.current_time = datetime.datetime.now().timestamp()
        #     self.udpate = False
        pass

    def congeston_control(self,packet):
        # if(self.current_time - datetime.datetime.now().timestamp() > self.udpate_time):
        #     self.udpate = False
        #     self.current_time = datetime.datetime.now().timestamp()
        #     self.current_cwnd = self.cwnd

        # if(self.current_cwnd > 0):
        #     self.current_cwnd -= 1
        #     return True 
        # else:
        #     return False

        return True
