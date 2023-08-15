class ConstConfig(object):
    NICE_VALUE = 10 # 0: normal, 10: easy to yield CPU to fg app

    MAX_EBPF_NUM_MSGS = 32
    
    MAX_GRPC_NUM_MSGS = 1024

    MAX_EXTENT_CACHE_SIZE = 2048 # 1024

    PAGE_DELETION_PAGE_CNT = 1024

    BTREE_DEGREE = 3 # number of nodes: t - 1 <= N <= 2t - 1
    
    EXTENT_SIZE = 32 * 1024 # XXX: hardcoded, please be same with poseidonos
    BLOCKS_PER_EXTENT = EXTENT_SIZE // 4096

