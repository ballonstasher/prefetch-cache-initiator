import os
import logging
import re 

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class PageDeletionInfo:
    def __init__(self, dev_id, ino, file_offset, file_size):
        self.dev_id = dev_id
        self.ino = ino
        self.file_offset = file_offset
        self.file_size = file_size
    
    def __repr__(self):
        return f"({self.dev_id}, {self.ino}, {self.file_offset}, {self.file_size}))"

## helper functions
def get_subsystem_id(subsysnqn):
    pattern = r"subsystem(\d+)"
    match = re.search(pattern, subsysnqn)
    if match:                          
        return int(match.group(1))  

def get_mount_info():
    with open('/proc/self/mounts', 'r') as file:
        for line in file:
            if line.startswith('/dev/nvme'):
                fields = line.split()
                return fields[0], fields[1]
        return None, None

def get_device_numbers(device_path):
    try:
        stat = os.stat(device_path)
        major = os.major(stat.st_rdev)
        minor = os.minor(stat.st_rdev)
        return (major << 20) | minor
    except FileNotFoundError:
        print("Error: {} does not exist" .format(device_path))
    except OSError as e:
        print("Error: Failed to get device numbers for {}: {}" 
                .format(device_path, e))

def get_nvme_info(dev_name):
    if "nvme" not in dev_name:
        return None, None

    ctrl_id_start = len("nvme")
    ns_id_start = dev_name.index("n", ctrl_id_start) + len("n")

    ctrl_id_end = ns_id_start - 1
    ns_id_end = len(dev_name)

    ctrl_id = int(dev_name[ctrl_id_start:ctrl_id_end])
    ns_id = int(dev_name[ns_id_start:ns_id_end])
    
    nvme_sysfs_path = "/sys/class/nvme/nvme" + str(ctrl_id)
    transport_path = nvme_sysfs_path + "/transport"
    try:
        with open(transport_path, "r") as f:
            transport = f.read().strip()
            if (transport == "pcie"): # local nvme
                return None, None
    except IOError as e:
        print("Failed to open ", transport_path)
        return None, None
    
    subsysnqn_path = nvme_sysfs_path + "/subsysnqn"
    try: 
        with open(subsysnqn_path, "r") as f:
            subsysnqn = f.read().strip()
            subsys_id = get_subsystem_id(subsysnqn)
            return subsys_id, ns_id
    except IOError as e:
        return None, None

def find_mnt_path(device):
    device = "/dev/" + device
    with open('/proc/self/mounts', 'r') as f:
        for line in f:
            fields = line.split()
            if fields[0] == device:
                return fields[1]


class Metadata:
    def __init__(self):
        self._dict = dict() # key=dev_id, value=[mnt_path, subsys_id, ns_id]
        self.initialize()

    def initialize(self):
        device_path, mount_path = get_mount_info()
        if device_path and mount_path:
            dev_id = get_device_numbers(device_path)
            self.put(dev_id, device_path.lstrip('/dev/'))

    def get_dev_ids(self):
        return list(self._dict.keys())

    def put(self, dev_id, dev_name):
        mnt_path = find_mnt_path(dev_name)
        if mnt_path is None:
            return False

        subsys_id, ns_id = get_nvme_info(dev_name)
        if subsys_id is None or ns_id is None:
            return False
        else: 
            self._dict[dev_id] = [mnt_path, subsys_id, ns_id]
            logging.info(f"mount: {dev_id} {dev_name} {mnt_path}")
            return True
    
    def get_mntpnt(self, dev_id):
        ret = self._dict.get(dev_id, None)
        if ret:
            return ret[0]

    def get_prefetch_meta(self, dev_id):
        ret = self._dict.get(dev_id, None)
        if ret:
            return ret[1], ret[2]
        else:
            return None, None

    def delete(self, dev_id):
        self._dict.pop(dev_id, None)
