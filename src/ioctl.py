import struct
import fcntl
import sys
import os
import array

from extent import *
from stats import *

struct_fiemap = '=QQLLLL'
#struct fiemap {
#	__u64	fm_start;	 /* logical offset (inclusive) at
#				  * which to start mapping (in) */
#	__u64	fm_length;	 /* logical length of mapping which
#				  * userspace cares about (in) */
#	__u32	fm_flags;	 /* FIEMAP_FLAG_* flags for request (in/out) */
#	__u32	fm_mapped_extents; /* number of extents that were
#				    * mapped (out) */
#	__u32	fm_extent_count; /* size of fm_extents array (in) */
#	__u32	fm_reserved;
#	struct fiemap_extent fm_extents[0]; /* array of mapped extents (out) */
#};

struct_fiemap_extent = '=QQQ2QL3L'
#struct fiemap_extent {
#	__u64	fe_logical;  /* logical offset in bytes for the start of
#			      * the extent */
#	__u64	fe_physical; /* physical offset in bytes for the start
#			      * of the extent */
#	__u64	fe_length;   /* length in bytes for the extent */
#	__u64	fe_reserved64[2];
#	__u32	fe_flags;    /* FIEMAP_EXTENT_* flags for this extent */
#	__u32	fe_reserved[3];
#};
sf_size = struct.calcsize(struct_fiemap)
sfe_size = struct.calcsize(struct_fiemap_extent)

FS_IOC_FIEMAP = 0xC020660B
FIEMAP_FLAG_SYNC = 0x00000001
# shift is for reporting in blocks instead of bytes
shift = 0#12

class Error(Exception):
    """A class for all the other exceptions raised by this module."""
    pass

class Fiemap:
    def __init__(self, file_path):
        self.file_path = file_path
        pass
    
    def parse_fiemap_extents(self, res, num_extents):
        fme_buf = res[sf_size:]
        extents = list()
        lba = 0
        for i in range(0, num_extents):
            offset = i * sfe_size
            extent = struct.unpack(struct_fiemap_extent, fme_buf[offset: offset + sfe_size]) 
            pba = extent[1]
            length = extent[2]

            extents.append(Extent(lba, pba, length))

            lba += length
            offset += sfe_size 
        return extents

    def fiemap_ioctl(self, fd, num_extents=0):
        # build fiemap struct
        buf = struct.pack(struct_fiemap, 
                0, 
                0xffffffffffffffff, 
                0, 
                0, 
                num_extents, 
                0)
        # add room for fiemap_extent struct array
        buf += b'\0' *  num_extents * sfe_size
        # use a mutable buffer to get around ioctl size limit
        buf = array.array('b', buf)
        # ioctl call
        ret = fcntl.ioctl(fd, FS_IOC_FIEMAP, buf)
        if ret == 0:
            return buf.tobytes()

    def do(self):
        try: 
            fd = open("%s" % self.file_path, "r")
            file_info = os.stat(self.file_path)
                
            # 1. get fm_extent_count
            res = self.fiemap_ioctl(fd)
            if res == 0:
                return None
            num_extents = struct.unpack(struct_fiemap, res[:sf_size])[3]

            # 2. do real fiemap
            res = self.fiemap_ioctl(fd, num_extents)
            if res is None:
                Stats.fiemap_ioctl_failed += 1
                return None
            else: 
                Stats.fiemap_succ += 1
                return self.parse_fiemap_extents(res, num_extents)

        except FileNotFoundError: # might be deleted or renamed
            #print("fiemap: file_nount_found={}" .format(self.file_path))
            Stats.fiemap_not_found_failed += 1
            pass
        except OSError as e:
            raise Error("open or stat file, err=%s" % e)


if __name__ == "__main__":
    #file_path = "/mnt/ssd/dummy_1GB.dat"
    file_path = "/mnt/nvme/test.txt"
    print("file name : %s" % file_path)

    extents = Fiemap(file_path).do()
    for extent in extents:
        print("LBA={}, PBA={}, LENGTH={}" .format(extent.lba, 
            extent.pba, extent.length))
