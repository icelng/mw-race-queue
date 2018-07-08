#include "queue_store.h"
#include <cstring>

using namespace std;
using namespace race2018;

#define FILE_SIZE_1G (1 * 1024 * 1024 * 1024L)
#define FILE_SIZE_2G (2 * FILE_SIZE_1G)
#define FILE_SIZE_4G (4 * FILE_SIZE_1G)
#define FILE_SIZE_200G (200 * FILE_SIZE_1G)
#define REGION_SIZE (2 * 1024 * 1024 * 1024L)
#define FILE_SIZE FILE_SIZE_200G
#define WRITE_BUFFERS_NUM 4
#define WRITE_BUFFERS_SIZE (128 * 1024 * 1024L)
#define QUEUE_TABLE_LEN 1000000

#include "commit_service.h"
#include "idle_page_manager.h"
#include "message_queue.h"
#include "store_io.h"
#include "message_queue.h"
#include "buffer_pool.h"


queue_store::queue_store() {
    store_io = new StoreIO("/alidata1/race2018/data/log", FILE_SIZE, REGION_SIZE, WRITE_BUFFERS_NUM, WRITE_BUFFERS_SIZE);
//    store_io = new StoreIO("./log", FILE_SIZE, REGION_SIZE, WRITE_BUFFERS_NUM, WRITE_BUFFERS_SIZE);
//    store_io = new StoreIO("./log", FILE_SIZE, REGION_SIZE);
    idle_page_manager = new IdlePageManager(FILE_SIZE, 4096);
//    buffer_pool = new BufferPool(16000000, 256);
    buffer_pool = new BufferPool(8400000, 512);
//    buffer_pool = new BufferPool(8000000, 512);
    commit_service = new CommitService(store_io, buffer_pool, 1);
    commit_service->start();
    queue_table = (MessageQueue**) malloc(QUEUE_TABLE_LEN * sizeof(MessageQueue*));
    for (int i = 0;i < QUEUE_TABLE_LEN;i++) {
        queue_table[i] = nullptr;
    }
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
void queue_store::put(const string& queue_name, const MemBlock& message) {
    MessageQueue *message_queue;
    long queue_id = strtol(&queue_name.c_str()[6], NULL, 10);

    if (queue_table[queue_id] == nullptr) {
        std::lock_guard<mutex> lock(queue_table_mutex);
        if (queue_table[queue_id] == nullptr) {
            queue_table[queue_id] = new MessageQueue(idle_page_manager, store_io, commit_service, buffer_pool);
        }
    }

    message_queue = queue_table[queue_id];

//    {
//        QueueTable::accessor a;
//        if (queue_map.insert(a, queue_id)) {
//            message_queue = new MessageQueue(idle_page_manager, store_io, commit_service, buffer_pool);
//            a->second = message_queue;
//        }
//        message_queue = a->second;
//    }

    message_queue->put(message);
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
vector<MemBlock> queue_store::get(const std::string& queue_name, long offset, long number) {
    MessageQueue *message_queue;

    long queue_id = strtol(&queue_name.c_str()[6], NULL, 10);

//    cout << "Find queue" << endl;
//    {
//        QueueTable::accessor a;
//        if (!queue_map.find(a, queue_id)) {
//            return vector<MemBlock>();
//        }
//        message_queue = a->second;
//    }

    message_queue = queue_table[queue_id];

    return message_queue->get(offset, number);
}