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

class StoreIO;
class IdlePageManager;
class CommitService;

using namespace race2018;

class MessageQueue {
public:
    MessageQueue(IdlePageManager *idlePageManager, StoreIO *store_io, CommitService *commit_service);
    void put(const race2018::MemBlock &mem_block);
    std::vector<race2018::MemBlock> get(long offset, long number);
    void do_commit();


private:
    void commit_later();
    void commit_now();
    void shortToBytes(unsigned short v, unsigned char b[], int off);
    unsigned short bytesToShort(unsigned char b[], int off);
    void expend_page_table();
    u_int64_t locate_msg_offset_in_page(void* page_start_ptr, u_int64_t msg_no);
    u_int32_t find_page_index(u_int64_t msg_index);

    IdlePageManager *idle_page_manager;
    StoreIO *store_io;
    CommitService *commit_service;
    size_t queue_len;
    bool is_need_commit;
    u_int32_t last_page_index;
    u_int32_t committing_page_index;
    sem_t commit_sem_lock;
    size_t commit_msg_num;
    u_int32_t need_commit_size;
    u_int64_t *page_table;
    size_t page_table_len;
    tbb::concurrent_queue<race2018::MemBlock> msg_put_queue;
    std::mutex mtx;
};

#endif //QUEUE_RACE_MESSAGE_QUEUE_H
