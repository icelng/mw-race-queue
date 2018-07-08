//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_MESSAGE_QUEUE_H
#define QUEUE_RACE_MESSAGE_QUEUE_H

#include <vector>
#include "unistd.h"
#include <iostream>
#include <memory>
#include <mutex>
#include "tbb/concurrent_queue.h"
#include "semaphore.h"
#include "queue_store.h"
#include "pthread.h"

class StoreIO;
class IdlePageManager;
class CommitService;
class BufferPool;

using namespace race2018;

class MessageQueue {
public:
    MessageQueue(IdlePageManager *idlePageManager, StoreIO *store_io, CommitService *commit_service, BufferPool *buffer_pool);
    void put(const race2018::MemBlock &mem_block);
    std::vector<race2018::MemBlock> get(long offset, long number);
    void do_commit();
    void commit_now();


private:
    void commit_later();
    void shortToBytes(unsigned short v, unsigned char b[], int off);
    unsigned short bytesToShort(unsigned char b[], int off);
    void expend_page_table();
    u_int64_t locate_msg_offset_in_page(void* page_start_ptr, u_int64_t msg_no);
    u_int32_t find_page_index(u_int64_t msg_index);

    IdlePageManager *idle_page_manager;
    StoreIO *store_io;
    CommitService *commit_service;
    BufferPool *buffer_pool;
    size_t queue_len;
    bool is_need_commit;
    u_int32_t last_page_index;
    u_int32_t committing_page_index;
    sem_t commit_sem_lock;
    size_t committing_size;
    size_t need_commit_size;
    u_int64_t *page_table;
    size_t page_table_len;
    void** commit_buffer_queue;
    std::atomic<long> commit_q_head;
    std::atomic<long> commit_q_tail;
    size_t max_commit_q_len;
//    tbb::concurrent_queue<void*> commit_buffer_queue;
    void* put_buffer;
    u_int64_t put_buffer_offset;
    size_t buffer_size;
    std::mutex mtx;
    std::atomic<int> hold_buffers_num;
};

#endif //QUEUE_RACE_MESSAGE_QUEUE_H
