#include "queue_store.h"

using namespace std;
using namespace race2018;

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
void queue_store::put(string queue_name, const MemBlock& message) {
    lock_guard<mutex> lock(mtx);
    queue_map[queue_name].push_back(message);
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
vector<MemBlock> queue_store::get(std::string queue_name, long offset, long number) {
    lock_guard<mutex> lock(mtx);
    if (queue_map.find(queue_name) == queue_map.end()) {
        return vector<MemBlock>();
    }
    auto queue = queue_map[queue_name];
    if (offset >= queue.size()) {
        return vector<MemBlock>();
    }

    vector<MemBlock> searchResult(queue.begin() + offset,
                            offset + number > queue.size() ? queue.end() : queue.begin() + offset + number);

    // Return deep copy of the MemBlock such that benchmark tool may release MemBlock::ptr safely.
    vector<MemBlock> ret;
    for (const auto &item : searchResult) {
        MemBlock block;
        block.size = item.size;
        block.ptr = new char[block.size];
        memcpy(block.ptr, item.ptr, block.size);
        ret.push_back(block);
    }

    return ret;
}