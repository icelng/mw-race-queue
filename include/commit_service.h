//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_COMMIT_SERVICE_H
#define QUEUE_RACE_COMMIT_SERVICE_H

#include "message_queue.h"
#include "semaphore.h"
#include "tbb/concurrent_queue.h"

class CommitService{
public:
    CommitService(unsigned int thread_num);
    void start();
    void request_commit(MessageQueue *messageQueue);
    void do_commit();
private:
    unsigned int thread_num;
    tbb::concurrent_queue<MessageQueue*> commit_queue;
    bool is_started;
    sem_t requesting_num;

};

#endif //QUEUE_RACE_COMMIT_SERVICE_H
