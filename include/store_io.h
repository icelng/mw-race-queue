//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_STORE_IO_H
#define QUEUE_RACE_STORE_IO_H

#include "tbb/concurrent_queue.h"
#include "semaphore.h"
#include <mutex>
#include <atomic>
#include "pthread.h"

struct FlushRequestNode {
    void* buffer;
    size_t flush_size;
};

class StoreIO {
public:
    StoreIO(const char *file_path,
            u_int64_t file_size,
            u_int64_t region_size,
            size_t buffers_num,
            size_t buffer_size);

    void* get_region(u_int64_t addr);
    void wait_flush_done();
    u_int32_t region_mask;
    void flush();
    void write_data(void *data, size_t data_size);
    void do_flush();
    void add_offset(u_int64_t offset);

private:

    void** regions;
    u_int32_t region_bits_len;
    u_int32_t region_size;
    u_int32_t regions_num;
    sem_t buffers_num;
    size_t buffer_size;
    void* buffer_now;
    u_int64_t buffer_offset;
//    tbb::concurrent_queue<FlushRequestNode> flush_queue;
    FlushRequestNode* flush_queue;
    std::atomic<long> flush_q_head;
    std::atomic<long> flush_q_tail;
    tbb::concurrent_queue<void*> buffers;
    pthread_spinlock_t flush_queue_lock;
    size_t max_flush_queue_len;
    std::atomic<long> flush_req_num_atomic;
    sem_t flush_req_num;
    sem_t is_flushing;
    int file_fd;
    std::mutex flush_mutex;
};

#endif //QUEUE_RACE_STORE_IO_H
