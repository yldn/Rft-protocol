import datetime



class flow_control():



    def __init__(self,data_rate):
        self.data = -1
        self.data_rate = data_rate
        self.last_time = datetime.datetime.now().timestamp()



    def flow_control(self,data_length):
        if(self.data_rate == 0):#TODO: change to max value of 8 Byte
            return True

        new_time = datetime.datetime.now().timestamp()
        if(( new_time - self.last_time ) >=1):
            self.data = date_rate
            self.last_time = new_time
        
        if(self.data-data_length<0):
            return False
        else:
            self.data -= data_length
            return True
        
        return True



    def change_flow_control_data_rate(self,data_rate):
        self.data_rate = data_rate