//
// Created by yiran on 18-7-5.
//


#include "commit_service.h"
#include "message_queue.h"
#include "store_io.h"
#include "pthread.h"
#include <iostream>

#define COMMIT_QUEUE_LEN 2000000

using namespace std;

CommitService::CommitService(StoreIO *store_io, BufferPool *buffer_pool, unsigned int thread_num) {
    this->thread_num = thread_num;
    this->store_io = store_io;
    this->buffer_pool = buffer_pool;
    sem_init(&this->requesting_num, 0, 0);
    is_started = false;
    commit_queue = (MessageQueue**) malloc(sizeof(void*) * COMMIT_QUEUE_LEN);
    head = 0;
    tail = 0;
    pthread_spin_init(&spinlock, 0);
    is_need_commit_all = true;
}

void *service(void *arg) {
    CommitService *commit_service = static_cast<CommitService *>(arg);

    while (true) {
        commit_service->do_commit();
    }


}

void CommitService::start() {

    if (is_started) {
        return;
    }

    cout << "Starting commit service...." << endl;
    for (int i = 0;i < thread_num;i++) {
        pthread_t tid;
        pthread_create(&tid, NULL, service, this);
    }

}

void CommitService::request_commit(MessageQueue *messageQueue) {
//    commit_queue.push(messageQueue);
    pthread_spin_lock(&spinlock);
    commit_queue[tail++ % COMMIT_QUEUE_LEN] = messageQueue;
    pthread_spin_unlock(&spinlock);
    sem_post(&requesting_num);
}

/**
 * 该方法仅供内部调用
 * */
void CommitService::do_commit() {

    sem_wait(&requesting_num);
    MessageQueue *message_queue;
//    if (!commit_queue.try_pop(message_queue)) {
//        cout << "Failed to pop message_queue when doing do_commit!" << endl;
//        return;
//    }

    pthread_spin_lock(&spinlock);
    message_queue = commit_queue[head++ % COMMIT_QUEUE_LEN];
    pthread_spin_unlock(&spinlock);

    if (message_queue == NULL) {
        cout << "The message_queue is NULL, please put a right point to the do_commit queue!" << endl;
        return;
    }

    message_queue->do_commit();

}

void CommitService::commit_all() {
    if (is_need_commit_all) {
        lock_guard<mutex> lock(mtx);
        if (is_need_commit_all) {
            MessageQueue *message_queue;
            while (need_commit.try_pop(message_queue)) {
                message_queue->commit_now();
            }
            is_need_commit_all = false;
            store_io->flush();
            buffer_pool->release_all();
        }
    }
}

void CommitService::set_need_commit(MessageQueue *message_queue) {
    need_commit.push(message_queue);
}


