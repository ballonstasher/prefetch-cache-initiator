from collections import defaultdict

from metadata import PageDeletionInfo
from stats import *
from config import *

import random 

class SteeringPolicy:
    STEERING_OFF = 0
    STEERING_BY_PATTERN = 1
    STEERING_BY_RANDOM = 2

def EXTENT_ID(file_offset):
    return file_offset // (ConstConfig.EXTENT_SIZE)

class Steering:
    def __init__(self, steering_on):
        self.dict = defaultdict(None)
        self.steering_on = steering_on

    def sort(self, input_list):
        return sorted(input_list, key=lambda x: (x.ino, EXTENT_ID(x.file_offset)))

    def classify(self, input_list):
        cur_inode = input_list[0].ino
        cur_extent_id = EXTENT_ID(input_list[0].file_offset)
        count = 1

        for i in range(1, len(input_list)):
            extent_id = EXTENT_ID(input_list[i].file_offset)
            
            if input_list[i].ino == cur_inode and extent_id == cur_extent_id:
                count += 1
            else:
                self.dict[(cur_inode, cur_extent_id)] = count

                cur_inode = input_list[i].ino
                cur_extent_id = extent_id
                count = 1
                
        # Print the last element
        self.dict[(cur_inode, cur_extent_id)] = count

    """
    def filter(self, input_list):
        used = (None, None)
        ret_list = []
        
        for x in input_list:
            inode = x.ino
            extent_id = EXTENT_ID(x.file_offset)
            
            count = self.dict[(inode, extent_id)]
            if count > (ConstConfig.BLOCKS_PER_EXTENT / 2):
                #if self.steering_on == SteeringPolicy.STEERING_BY_PATTERN:
                Stats.steering_to_storage += 1
                if used == (inode, extent_id):
                    self.dict[(inode, extent_id)] = count - 1
                    continue
                else:
                    if used[0] is not None:
                        if count > 1:
                             self.dict[(inode, extent_id)] = count - 1
                        else:
                            del self.dict[(inode, extent_id)]
                    
                    used = (inode, extent_id)
                    ret_list.append(x)
            else:
                if self.steering_on == SteeringPolicy.STEERING_OFF:
                    ret_list.append(x)
                 
                if count > 1:
                     self.dict[(inode, extent_id)] = count - 1
                else:
                    del self.dict[(inode, extent_id)]
                     
                #if self.steering_on == SteeringPolicy.STEERING_BY_PATTERN:
                Stats.steering_to_memory += 1
        
        print(self.dict)

        return ret_list
    """

    def filter(self, input_list):
        used = (None, None)
        ret_list = []
        num_remains = -1

        for x in input_list:
            inode = x.ino
            extent_id = EXTENT_ID(x.file_offset)
            
            #print(f"{inode} {x.file_offset} {extent_id}")
            count = self.dict[(inode, extent_id)]
            if num_remains == -1:
                num_remains = count
            
            num_remains -= 1

            if count > (ConstConfig.BLOCKS_PER_EXTENT // 3):
                Stats.steering_to_storage += 1
                if used == (inode, extent_id):
                    if num_remains == 0:
                        num_remains = -1
                        del self.dict[(inode, extent_id)]
                    continue
                else:
                    used = (inode, extent_id)
                    ret_list.append(x)
            else:
                # FIXME
                if self.steering_on == SteeringPolicy.STEERING_OFF:
                    ret_list.append(x)
                 
                if num_remains == 0:
                    num_remains = -1
                    del self.dict[(inode, extent_id)]
                     
                Stats.steering_to_memory += 1
        
        #print(self.dict)

        return ret_list


    def filter_random(self, input_list):
        k = random.randint(1, len(input_list))
        ret_list = random.sample(input_list, k)
        
        Stats.steering_to_storage += k
        Stats.steering_to_memory += (len(input_list) - k)

        return ret_list


    def process(self, msg_list):
        if self.steering_on <= SteeringPolicy.STEERING_BY_PATTERN:
            sorted_input_list = self.sort(msg_list)
            self.classify(sorted_input_list)
            return self.filter(sorted_input_list)
        elif self.steering_on == SteeringPolicy.STEERING_BY_RANDOM:
            # TODO: don't have to sort
            # randomly select pages
            return self.filter_random(msg_list)
            
        

if __name__ == "__main__":
    steering = Steering(SteeringPolicy.STEERING_BY_PATTERN)
    
    dev_id = 0
    file_size = 0
    
    input_list = [
            PageDeletionInfo(dev_id, 5, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 2 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 4, 3 * ConstConfig.EXTENT_SIZE, file_size),
            ]
    ret_list = steering.process(input_list)
    print("=== Storage ===")
    print(ret_list)

    input_list = [
            PageDeletionInfo(dev_id, 5, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 4, 1 * ConstConfig.EXTENT_SIZE, file_size),
            ]
    ret_list = steering.process(input_list)
    print("=== Storage ===")
    print(ret_list)

    input_list = [
            PageDeletionInfo(dev_id, 5, 3 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 4, 0 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 5, 3 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 5, 4 * ConstConfig.EXTENT_SIZE, file_size),
            ]
    ret_list = steering.process(input_list)
    print("=== Storage ===")
    print(ret_list)
        
    """    
    # random selection
    steering.steering_on = SteeringPolicy.STEERING_BY_RANDOM
    
    input_list = [
            PageDeletionInfo(dev_id, 5, 3 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 4, 0 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 1, 1 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 5, 3 * ConstConfig.EXTENT_SIZE, file_size),
            PageDeletionInfo(dev_id, 5, 4 * ConstConfig.EXTENT_SIZE, file_size),
            ]

    ret_list = steering.process(input_list)
    print("=== Storage ===")
    print(ret_list)
    """
