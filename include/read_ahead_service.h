//
// Created by yiran on 18-7-11.
//


#ifndef QUEUE_RACE_READ_AHEAD_SERVICE_H
#define QUEUE_RACE_READ_AHEAD_SERVICE_H

#include "unistd.h"
#include <iostream>
#include <atomic>
#include "semaphore.h"
#include "pthread.h"

class MessageQueue;

class ReadAheadService {
public:
    ReadAheadService(size_t thread_num);

    void request_read_ahead(MessageQueue *message_queue);
    void do_read_ahead();
private:

    MessageQueue** request_queue;
    size_t max_request_q_len;
    std::atomic<long> head;
    std::atomic<long> tail;
    sem_t request_num;
    pthread_spinlock_t queue_spin_lock;

};

#endif //QUEUE_RACE_READ_AHEAD_SERVICE_H

