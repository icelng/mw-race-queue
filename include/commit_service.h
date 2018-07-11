//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_COMMIT_SERVICE_H
#define QUEUE_RACE_COMMIT_SERVICE_H

#include "tbb/concurrent_queue.h"
#include "semaphore.h"
#include "buffer_pool.h"
#include <mutex>

class StoreIO;
class MessageQueue;
class BufferPool;

class CommitService{
public:
    CommitService(StoreIO *store_io, BufferPool *buffer_pool, unsigned int thread_num);
    void start();
    void request_commit(MessageQueue *messageQueue);
    void do_commit();
    void commit_all();
    void set_need_commit(MessageQueue *message_queue);

//    std::mutex commit_mutex;
    pthread_mutex_t commit_mutex;
private:
    unsigned int thread_num;
    StoreIO *store_io;
    MessageQueue** commit_queue;
    BufferPool *buffer_pool;
//    tbb::concurrent_queue<MessageQueue*> commit_queue;
    tbb::concurrent_queue<MessageQueue*> need_commit;
    bool is_started;
    sem_t requesting_num;
    u_int64_t head;
    u_int64_t tail;
    pthread_spinlock_t spinlock;
    bool is_need_commit_all;
    std::mutex mtx;

};

#endif //QUEUE_RACE_COMMIT_SERVICE_H
