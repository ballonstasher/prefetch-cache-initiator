from bcc import BPF
from bcc.utils import printb
from collections import defaultdict
import os
from ctypes import *

import threading
import queue 

from metadata import *
from stats import *
from config import *

bpf_text_mount = """
#include <uapi/linux/ptrace.h>
#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/genhd.h>

BPF_HASH(mnt_map, u32, u8, 1024);

struct mount_event_info {
    u32 dev_id;
    char dev_name[DISK_NAME_LEN];
};

BPF_PERF_OUTPUT(mount_events);

int traceret_mount_bdev(struct pt_regs *ctx)
{
    struct dentry *dentry = (struct dentry *) PT_REGS_RC(ctx);
    struct super_block *sb = dentry->d_sb;

    if (dentry) {
        struct mount_event_info info = {};
        char *dev_name = sb->s_bdev->bd_disk->disk_name;
        u32 dev_id = sb->s_dev;
        u8 value = 1;

        mnt_map.update(&dev_id, &value);

        info.dev_id = dev_id;

        bpf_probe_read_kernel_str(&info.dev_name, sizeof(info.dev_name), dev_name);
        mount_events.perf_submit(ctx, &info, sizeof(info));
    }

    return 0;
}

int trace_generic_shutdown_super(struct pt_regs *ctx, struct super_block *sb)
{
    struct mount_event_info info;
    u32 dev_id = sb->s_dev;

    mnt_map.delete(&dev_id);
    
    info.dev_id = dev_id;
    
    mount_events.perf_submit(ctx, &info, sizeof(info.dev_id));

    return 0;
}
"""

bpf_text_page_deletion = """
#include <uapi/linux/ptrace.h>
#include <linux/page-flags.h>
#include <linux/fs.h>

struct page_deletion_event_info {
    u32 dev_id;
    u64 ino;
    u64 file_offset;
    u64 file_size; 
    //u8 referenced;
    //u8 workingset;
};

BPF_PERF_OUTPUT(page_deletion_events);

int trace_unaccount_page_cache_page(struct pt_regs *ctx, struct address_space *mapping, 
    struct page *page)
{
    /* check file-backed clean page */
    //if ((page->flags & PG_uptodate) && (page->flags & PG_mappedtodisk)) {
    if ((page->flags & (1UL << PG_uptodate)) && 
            (page->flags & (1UL << PG_mappedtodisk))) {
        struct inode *inode = mapping->host;
        struct page_deletion_event_info info = {};
        u32 dev_id = inode->i_sb->s_dev;
        u8 *ret;

        ret = mnt_map.lookup(&dev_id);
        if (!ret) {
            return 0;
        }
        
        /* XXX : referenced page goes to Ballonstasher */
        //if (page->flags & (1UL << PG_referenced)) {
        //    info.referenced = true;
        //} else {
        //    info.referenced = false;
        //} 
        //if (page->flags & (1UL << PG_workingset)) {
        //    info.workingset = true;
        //} else {
        //    info.workingset = false;
        //}

        info.dev_id = dev_id; 
        info.ino = inode->i_ino;
        info.file_offset = page->index << PAGE_SHIFT;
        info.file_size = inode->i_size; 
        page_deletion_events.perf_submit(ctx, &info, sizeof(info));
    }

    return 0;
}
"""

bpf_text_open = """
#include <linux/namei.h>
#include <linux/sched.h>
#include <linux/fs_struct.h>
#include <linux/dcache.h>
#include <linux/fs.h>

#define MAX_ENTRIES 32

enum event_type {
    EVENT_OPEN_ENTRY = 0,
    EVENT_OPEN_END,
    EVENT_CLOSE
};

enum filepath_type {
    FPATH_ABSOLUTE = 0,
    FPATH_ABSOLUTE_LONG,
    FPATH_RELATIVE,
};

struct val_t {
    u64 id;
    char comm[TASK_COMM_LEN];
    const char *fname;
};

struct data_t {
    u64 id;
    u64 ino;
    u32 dev_id;
    char comm[TASK_COMM_LEN];
    enum event_type type;
    enum filepath_type fpath_type;
    char name[NAME_MAX];
};

struct open_flags {  
    int open_flag;   
    umode_t mode;    
    int acc_mode;    
    int intent;      
    int lookup_flags;
};                   

BPF_PERF_OUTPUT(open_events);
BPF_HASH(infotmp, u64, struct val_t);

int trace_do_filp_open(struct pt_regs *ctx, 
    int dfd, struct filename *pathname, const struct open_flags *op)
{
    struct val_t val = {};
    u64 id = bpf_get_current_pid_tgid();
    
#if 0
    if (container_should_be_filtered()) {
        return 0;
    }
#endif 
    
    // TODO: ignore lookup directory 

    if (bpf_get_current_comm(&val.comm, sizeof(val.comm)) == 0) {
        val.id = id;
        val.fname = pathname->name;
        infotmp.update(&id, &val);
    }

    return 0;
}

int traceret_do_filp_open(struct pt_regs *ctx)
{
    struct file *file = (struct file *) PT_REGS_RC(ctx);
    u64 id = bpf_get_current_pid_tgid();
    struct val_t *valp;
    u32 dev_id = file->f_inode->i_sb->s_dev;
    u8 *ret;
    bool is_fpath_abs_long = false;

    valp = infotmp.lookup(&id);
    if (valp == 0) {
        // missed entry
        return 0;
    }
    
    ret = mnt_map.lookup(&dev_id);
    if (!ret) {
        // not target dev
        return 0;
    }

    struct data_t data = {};
    bpf_probe_read_kernel(&data.comm, sizeof(data.comm), valp->comm);
    bpf_probe_read_kernel_str(&data.name, sizeof(data.name), (void *) valp->fname); // TODO: ??!! how to handle absoulte long?
    data.id = valp->id;
    data.ino = file->f_inode->i_ino;
    data.dev_id = dev_id;

    if (data.name[0] == '/' && data.name[NAME_MAX - 1]) { 
        is_fpath_abs_long = true; 
    }

    data.type = EVENT_OPEN_ENTRY;
    data.fpath_type = FPATH_ABSOLUTE;
    open_events.perf_submit(ctx, &data, sizeof(data));

    if (data.name[0] != '/') { // relative path
        struct task_struct *task;
        struct dentry *dentry;
        int i;

        task = (struct task_struct *)bpf_get_current_task();
        dentry = task->fs->pwd.dentry;

        for (i = 0; i < MAX_ENTRIES; i++) {
            bpf_probe_read_kernel(&data.name, sizeof(data.name), (void *)dentry->d_name.name);
            data.type = EVENT_OPEN_ENTRY;
            open_events.perf_submit(ctx, &data, sizeof(data));

            if (dentry == dentry->d_parent) { // root directory (until mntpnt, not the actual root)
                break;
            }

            dentry = dentry->d_parent;
        }
        data.fpath_type = FPATH_RELATIVE;
    }

    if (is_fpath_abs_long) {
        int i;
        const char *ptr = valp->fname + NAME_MAX;
        for (i = 1; i < MAX_ENTRIES; i++) {
            bpf_probe_read_kernel(&data.name, sizeof(data.name), (void *) ptr);
            data.type = EVENT_OPEN_ENTRY;
            open_events.perf_submit(ctx, &data, sizeof(data));

            if (data.name[NAME_MAX - 1] == 0) {
                break;
            }
            ptr += NAME_MAX;
        }
        data.fpath_type = FPATH_ABSOLUTE_LONG;
    }

    data.type = EVENT_OPEN_END;
    open_events.perf_submit(ctx, &data, sizeof(data));

    infotmp.delete(&id);

    return 0;
}

// XXX: do not trace close
int trace_filp_close(struct pt_regs *ctx, struct file *filp, fl_owner_t id)
{  
#if 0
    u32 dev_id = filp->f_inode->i_sb->s_dev;
    u8 *ret;
    
    ret = mnt_map.lookup(&dev_id);
    if (!ret) {
        // not target dev
        return 0;
    }
    
    struct data_t data = {};
    data.type = EVENT_CLOSE;
    data.ino = filp->f_inode->i_ino; 
    data.dev_id = dev_id;
    open_events.perf_submit(ctx, &data, sizeof(data));
#endif
    return 0;
}
"""

bpf_prog = bpf_text_mount + bpf_text_page_deletion + bpf_text_open

bpf = BPF(text=bpf_prog)

mount_fnname = "mount_bdev"
umount_fnname = "generic_shutdown_super"

open_fname = "do_filp_open"
close_fname = "filp_close"

page_deletion_fname = "unaccount_page_cache_page"

bpf.attach_kretprobe(event = mount_fnname, 
        fn_name = "traceret_" + mount_fnname)
bpf.attach_kprobe(event = umount_fnname, 
        fn_name = "trace_" + umount_fnname)

bpf.attach_kprobe(event = open_fname, fn_name = "trace_" + open_fname)
bpf.attach_kretprobe(event = open_fname, fn_name = "traceret_" + open_fname)
bpf.attach_kprobe(event = close_fname, fn_name = "trace_" + close_fname)

bpf.attach_kprobe(event = page_deletion_fname, 
        fn_name = "trace_" + page_deletion_fname)


class CallbackMount:
    def __init__(self, meta_dict):
        self.meta_dict = meta_dict

    def callback(self, cpu, data, size):
        event = bpf["mount_events"].event(data)
    
        dev_id = event.dev_id
        if event.dev_name:
            dev_name = event.dev_name.decode('utf-8')
            self.meta_dict.put(dev_id, dev_name)
        else:
            self.meta_dict.delete(dev_id)


class CallbackPageDeletion:
    def __init__(self, event_queue):
        self._event_queue = event_queue
        self._msg_list = []
        self._num_msgs = 0

    def lost_cb(self, count):
        Stats.lost_page_deletion += 1
        pass

    def callback(self, cpu, data, size):
        event = bpf["page_deletion_events"].event(data)
        #print(f"page_deletion: {event.ino} {event.file_offset}")
        
        #if event.referenced:
        #    Stats.page_deletion_referenced += 1
        #else:
        #    Stats.page_deletion_unreferenced += 1
        #if event.workingset:
        #    Stats.page_deletion_workingset += 1
        #else:
        #    Stats.page_deletion_not_workingset += 1

        msg = PageDeletionInfo(event.dev_id, event.ino, event.file_offset, 
                event.file_size) 

        self._msg_list.append(msg)
        self._num_msgs += 1
        
        """ XXX: need some method to filter random offset """

        if self._num_msgs == ConstConfig.MAX_EBPF_NUM_MSGS:
            try:
                self._event_queue.put(self._msg_list, block=False)
                Stats.page_deletion += ConstConfig.MAX_EBPF_NUM_MSGS
            except queue.Full:
                Stats.lost_queue_full_page_deletion += ConstConfig.MAX_EBPF_NUM_MSGS
                pass

            self._msg_list = []
            self._num_msgs = 0

# enum style
class EventType(object):
    EVENT_OPEN_ENTRY = 0
    EVENT_OPEN_END = 1
    EVENT_CLOSE = 2

class FilePathType(object):
    FPATH_ABSOLUTE = 0
    FPATH_ABSOLUTE_LONG = 1
    FPATH_RELATIVE = 2

class CallbackOpenClose:
    def __init__(self, fpath_dict):
        self._fpath_dict = fpath_dict
        self._entries = defaultdict(list)
    
    def lost_cb(self, count):
        Stats.lost_open += 1
        pass

    def callback(self, cpu, data, size):
        event = bpf["open_events"].event(data)

        dev_id = event.dev_id
        ino = event.ino
        tgid = event.id
        name = event.name 

        if event.type == EventType.EVENT_OPEN_END:
            paths = self._entries[tgid]
            if paths is None:
                return

            path_type = FilePathType.FPATH_ABSOLUTE
            if (event.fpath_type == FilePathType.FPATH_RELATIVE):
                path_type = FilePathType.FPATH_RELATIVE
                paths.reverse()
                file_path = os.path.join(*paths)
                Stats.open_rel += 1
            elif event.fpath_type == FilePathType.FPATH_ABSOLUTE_LONG:
                file_path = b''.join(paths) # TODO: ??
                Stats.open_abs_long += 1
            else:
                file_path = name
                Stats.open_abs += 1

            self._fpath_dict[(ino, dev_id)] = (path_type, file_path)
            #print("({} {}) {} {}" .format(ino, dev_id, path_type, file_path))
            
            try:
                del(self._entries[tgid])
            except Exception:
                pass

        elif event.type == EventType.EVENT_OPEN_ENTRY:
            self._entries[tgid].append(name)
            
        elif event.type == EventType.EVENT_CLOSE:
            #self._fpath_dict.pop((ino, dev_id), None)
            pass
        else:
            assert 0, "Unknown event type"


class BPFTracer(threading.Thread):
    def __init__(self, event_queue, meta_dict, fpath_dict):
        threading.Thread.__init__(self)
        super().__init__()
        self.name = "BPFTracer"
        
        self._event_queue = event_queue
        self._meta_dict = meta_dict
        self._fpath_dict = fpath_dict

    def run(self):
        mnt_map = bpf.get_table("mnt_map")
        dev_ids = self._meta_dict.get_dev_ids() 
        for dev_id in dev_ids:
            mnt_map[c_int(dev_id)] = c_int(1)
        
        bpf["mount_events"].open_perf_buffer(
                CallbackMount(self._meta_dict).callback, page_cnt=2)
        
        callback_page_deletion = CallbackPageDeletion(self._event_queue)
        bpf["page_deletion_events"].open_perf_buffer(
                callback_page_deletion.callback, 
                page_cnt=ConstConfig.PAGE_DELETION_PAGE_CNT,
                lost_cb=callback_page_deletion.lost_cb)

        callback_open_close = CallbackOpenClose(self._fpath_dict)
        bpf["open_events"].open_perf_buffer(
                callback_open_close.callback, 
                page_cnt=128, 
                lost_cb=callback_open_close.lost_cb)
       
        while True:
            try:
                bpf.perf_buffer_poll()
            except KeyboardInterrupt:
                break
            except ValueError:
                continue

# TODO: test program
if __name__ == '__main__':
    pass
