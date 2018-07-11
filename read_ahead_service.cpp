//
// Created by yiran on 18-7-11.
//

#include <malloc.h>
#include "read_ahead_service.h"
#include "message_queue.h"

#define MAX_READ_AHEAD_QUEUE_LEN 1000000

void* read_ahead_service(void *arg) {
    ReadAheadService *read_ahead_service = (ReadAheadService*) arg;

    while (true) {
        read_ahead_service->do_read_ahead();
    }

}

ReadAheadService::ReadAheadService(size_t thread_num) {
    max_request_q_len = MAX_READ_AHEAD_QUEUE_LEN;
    request_queue = (MessageQueue**) malloc(sizeof(void*) * (max_request_q_len + 1));
    head = 0;
    tail = 0;
    sem_init(&request_num ,0 ,0);
    pthread_spin_init(&queue_spin_lock, 0);

    pthread_t tid;
    for (int i = 0;i < thread_num;i++) {
        pthread_create(&tid, NULL, read_ahead_service, this);
    }
}

void ReadAheadService::request_read_ahead(MessageQueue *message_queue) {

    pthread_spin_lock(&queue_spin_lock);
    request_queue[tail++ % (max_request_q_len + 1)] = message_queue;
    pthread_spin_unlock(&queue_spin_lock);

    sem_post(&request_num);

}

void ReadAheadService::do_read_ahead() {
    MessageQueue *message_queue;

    sem_wait(&request_num);
    pthread_spin_lock(&queue_spin_lock);
    message_queue = request_queue[head++ % (max_request_q_len + 1)];
    pthread_spin_unlock(&queue_spin_lock);
    message_queue->do_read_ahead();

}
