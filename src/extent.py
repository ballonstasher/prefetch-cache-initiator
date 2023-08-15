class Extent:
    def __init__(self, lba, pba, length):
        self.lba = lba
        self.pba = pba
        self.length = length

def extent_hold_key(extent, pos):
    return extent.lba <= pos and pos <= (extent.lba + extent.length - 1) 


def gen_extent_list(file_size):
    import random
    import numpy as np

    np.random.seed(0)

    page_size = 4096
    mean = 4 * page_size
    std_dev = page_size
    lengths = np.random.normal(mean, std_dev, size=10000)
    # round to nearest multiple of 4096
    lengths = (lengths // page_size) * page_size  
    
    extents = []
    lba = 0
    total_length = 0
    done = 0
    for i, length in enumerate(lengths):
        total_length += int(length)
        
        if (total_length >= file_size):
            if (total_length > file_size):
                old_total_length = total_length - int(length)
                length = file_size - old_total_length
                total_length = old_total_length + length
            done = 1

        extent = Extent(lba, lba, int(length))
        extents.append(extent)
        #print(f"Extent {i}: LBA={extent.lba}, PBA={extent.pba}, 
        #        Length={extent.length}")

        if (done):
            break

        lba += int(length)

    return extents


def main():
    file_size = 1 << 20
    
    extents = gen_extent_list(file_size)

    # Print extents
    total_length = 0
    for extent in extents:
        total_length += extent.length
        print(f"LBA: {extent.lba}, PBA: {extent.pba}, Length: {extent.length}")
    
    if (file_size != total_length):
        print("file_size != total_length {} {}" .format(file_size, total_length))

if __name__ == "__main__":
    main()
