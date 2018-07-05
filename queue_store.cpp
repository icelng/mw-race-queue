#include "queue_store.h"
#include <cstring>

using namespace std;
using namespace race2018;

#define FILE_SIZE_1G (1 * 1024 * 1024 * 1024L)
#define FILE_SIZE_2G (2 * FILE_SIZE_1G)
#define FILE_SIZE_4G (4 * FILE_SIZE_1G)
#define FILE_SIZE_200G (200 * FILE_SIZE_1G)
#define REGION_SIZE (1024 * 1024 * 1024)
#define FILE_SIZE FILE_SIZE_1G

#include "commit_service.h"
#include "idle_page_manager.h"
#include "message_queue.h"
#include "store_io.h"
#include "message_queue.h"

queue_store::queue_store() {
    store_io = new StoreIO("./log", FILE_SIZE, REGION_SIZE);
    idle_page_manager = new IdlePageManager(FILE_SIZE, 4096);
    commit_service = new CommitService(8);
    commit_service->start();
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
void queue_store::put(const string& queue_name, const MemBlock& message) {
    tbb::concurrent_hash_map<std::string, MessageQueue*>::accessor a;
    MessageQueue *message_queue;

    if (queue_map.insert(a, queue_name)) {
        message_queue = new MessageQueue(idle_page_manager, store_io, commit_service);
        a->second = message_queue;
    }

    message_queue = a->second;
    message_queue->put(message);
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
vector<MemBlock> queue_store::get(const std::string& queue_name, long offset, long number) {
    tbb::concurrent_hash_map<std::string, MessageQueue*>::accessor a;
    MessageQueue *message_queue;

    if (!queue_map.find(a, queue_name)) {
        return vector<MemBlock>();
    }

    message_queue = a->second;

    return message_queue->get(offset, number);
}