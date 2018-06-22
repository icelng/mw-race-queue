#include "queue_store.h"

using namespace std;
using namespace race2018;

MemBlock::MemBlock(const MemBlock& other) {
    m_data = other.m_data;
    m_length = other.m_length;
    ref = other.ref;
    if (m_data) {
        ref->fetch_add(1);
    }
}

MemBlock::~MemBlock() {
    if (ref) {
        ref->fetch_sub(1);
        if (0 == ref->load()) {
            delete(m_data);
            delete(ref);
        }
    }
}

void queue_store::put(string queue_name, const MemBlock& message) {
    lock_guard<mutex> lock(mtx);
    queue_map[queue_name].push_back(message);
}

vector<MemBlock> queue_store::get(std::string queue_name, long offset, long number) {
    lock_guard<mutex> lock(mtx);
    if (queue_map.find(queue_name) == queue_map.end()) {
        return vector<MemBlock>();
    }
    auto queue = queue_map[queue_name];
    return vector<MemBlock>(queue.begin() + offset, queue.begin() + offset + number);
}