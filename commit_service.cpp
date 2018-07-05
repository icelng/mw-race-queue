//
// Created by yiran on 18-7-5.
//


#include "commit_service.h"
#include "message_queue.h"
#include "pthread.h"
#include <iostream>

using namespace std;

CommitService::CommitService(unsigned int thread_num) {
    this->thread_num = thread_num;
    sem_init(&this->requesting_num, 0, 0);
    is_started = false;
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

    for (int i = 0;i < thread_num;i++) {
        pthread_t tid;
        pthread_create(&tid, NULL, service, this);
    }

}

void CommitService::request_commit(MessageQueue *messageQueue) {
    commit_queue.push(messageQueue);
    sem_post(&requesting_num);
}

/**
 * 该方法仅供内部调用
 * */
void CommitService::do_commit() {

    sem_wait(&requesting_num);
    MessageQueue *message_queue;
    if (!commit_queue.try_pop(message_queue)) {
        cout << "Failed to pop message_queue when doing do_commit!" << endl;
        return;
    }

    if (message_queue == NULL) {
        cout << "The message_queue is NULL, please put a right point to the do_commit queue!" << endl;
    }

    message_queue->do_commit();

}


