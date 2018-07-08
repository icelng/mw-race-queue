//
// Created by yiran on 18-7-6.
//

#ifndef QUEUE_RACE_BUFFER_POOL_H
#define QUEUE_RACE_BUFFER_POOL_H

#include <atomic>
#include "semaphore.h"

class BufferPool {
public:
    BufferPool(u_int64_t pool_size, u_int64_t buffer_size);

    void* borrow_buffer();
    void return_buffer(void*);
    u_int32_t get_buffer_size();
    int get_remain_buffers_num();
    void release_all();
    void* borrow_page();  // 有借无还的

private:
    void* memory;

    sem_t remain_buffer_num;
    u_int64_t buffer_size;
    void **buffers;
    size_t max_queue_length;
    std::atomic<long> head;
    std::atomic<long> tail;
    std::atomic<unsigned long> page_get_index;
    pthread_spinlock_t spinlock;
//    tbb::concurrent_queue<void*> buffers;
};

#endif //QUEUE_RACE_BUFFER_POOL_H
