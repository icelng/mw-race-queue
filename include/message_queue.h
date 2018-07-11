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
class ReadAheadService;

using namespace race2018;

struct PageEntry {
    u_int64_t addr;
    u_int64_t queue_len;
};

struct ReadCache {
    void* page_cache;
    u_int64_t phy_address;
};

class MessageQueue {
public:
    MessageQueue(IdlePageManager *idlePageManager,
                 StoreIO *store_io,
                 CommitService *commit_service,
                 ReadAheadService *read_ahead_service,
                 BufferPool *buffer_pool);

    void put(const race2018::MemBlock &mem_block);
    std::vector<race2018::MemBlock> get(long offset, long number);
    void do_read_ahead();
    void do_commit();
    void commit_now();


private:
    void commit_later();
    void shortToBytes(unsigned short v, unsigned char b[], int off);
    void accumulate_to_buffers(const MemBlock &mem_block);
    unsigned short bytesToShort(unsigned char b[], int off);
    void expend_page_table();
    u_int64_t locate_msg_offset_in_page(void* page_start_ptr, u_int64_t msg_no);
    u_int32_t find_page_index(u_int64_t msg_index);

    IdlePageManager *idle_page_manager;
    StoreIO *store_io;
    CommitService *commit_service;
    ReadAheadService *read_ahead_service;
    BufferPool *buffer_pool;
    size_t queue_len;
    bool is_need_commit;
    u_int32_t page_size;
    u_int32_t last_page_index;
    u_int32_t committing_page_index;
    sem_t commit_sem_lock;
    size_t committing_size;
    size_t need_commit_size;
    PageEntry *page_table;
    size_t page_table_len;
    void** commit_buffer_queue;
    long commit_q_head;
    long commit_q_tail;
    size_t max_commit_q_len;
//    tbb::concurrent_queue<void*> commit_buffer_queue;
    void* put_buffer;
    u_int64_t put_buffer_offset;
    size_t buffer_size;
    size_t buffers_num_per_page;
    std::mutex mtx;

    bool is_read_cache_actived;
    ReadCache* read_cache_queue;
    std::atomic<long> read_cache_num;
    size_t max_rc_q_len;
    u_int64_t rc_q_head;
    u_int64_t rc_q_tail;
    pthread_mutex_t read_cache_q_lock;
    u_int64_t read_cache_trigger;
    long last_read_index;
    u_int64_t last_read_page_address;
    u_int64_t last_read_offset_in_page;
    sem_t read_ahead_sem_lock;
    u_int32_t ra_start_page_index;
};

#endif //QUEUE_RACE_MESSAGE_QUEUE_H
