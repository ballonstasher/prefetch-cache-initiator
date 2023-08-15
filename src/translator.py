#import threading
import queue 
import asyncio
from functools import lru_cache

from metadata import *
from extent_tree import *
from ioctl import *
from lru_cache import *
from stats import *
from config import *

class TransInfo:
    def __init__(self, subsys_id, ns_id, pba):
        self.subsys_id = subsys_id
        self.ns_id = ns_id
        self.pba = pba

class Translator():
    def __init__(self, meta_dict, fpath_dict):
        self._queue = queue.Queue()
        self._meta_dict = meta_dict
        self._fpath_dict = fpath_dict
        """
                                      [0]         [1]
        key: (ino, dev_id), value: (file_size, extent_tree)
        """
        self._lru_cache = LRUCache(ConstConfig.MAX_EXTENT_CACHE_SIZE) 
    
    def __del__(self):
        self._queue.join()

    def find_file_path_fast(self, ino, dev_id):
        ret = self._fpath_dict.get((ino, dev_id))
        if ret is None:
            return None

        path_type = ret[0]
        file_path = ret[1].decode('utf-8');

        if path_type == 2:
            mnt_path = self._meta_dict.get_mntpnt(dev_id) 
            return mnt_path + file_path
        else: 
            return file_path

    def build_tree(self, ino, dev_id):
        file_path = self.find_file_path_fast(ino, dev_id)
        if file_path is None:
            return None
        
        extents = Fiemap(file_path).do()
        if extents is None:
            return None

        extent_tree = ExtentTree(ConstConfig.BTREE_DEGREE)
        # insert extents to extent_tree
        for extent in extents:
            extent_tree.insert(Item(extent.lba, extent))

        return extent_tree
    
    # get cached extent_tree or building extent tree
    def trans_pba(self, info):
        dev_id = info.dev_id
        ino = info.ino
        lba = info.file_offset
        file_size = info.file_size
        
        extent_tree = None

        #print("inode={}, lba={}" .format(ino, lba))
        value = self._lru_cache.get((ino, dev_id))
        if value is None:
            extent_tree = self.build_tree(ino, dev_id)
            if extent_tree:
                ev_key = self._lru_cache.put((ino, dev_id), 
                        (file_size, extent_tree))
                if ev_key:
                    self._fpath_dict.pop((ev_key[0], ev_key[1]), None)
        else:
            # check the file modification
            if value[0] != file_size:
                #print("extent modified: ino={}" .format(ino))
                self._lru_cache.delete((ino, dev_id))
                extent_tree = self.build_tree(ino, dev_id)
                if extent_tree:
                    ev_key = self._lru_cache.put((ino, dev_id), 
                            (file_size, extent_tree))
                    if ev_key:
                        self._fpath_dict.pop((ev_key[0], ev_key[1]), None)
            else:
                extent_tree = value[1]
       
        if extent_tree:
            item = extent_tree.find(Item(lba))
            if item:
                extent = item.v
                Stats.ext_found += 1
                return extent.pba + (lba - extent.lba) # pba + offset
            else:
                Stats.ext_not_found += 1
                pass

