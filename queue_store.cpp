#include "queue_store.h"

using namespace std;
using namespace race2018;

MemBlock::MemBlock(const MemBlock& other) {
    m_data = other.m_data;
    m_length = other.m_length;
    ref = other.ref;
    if (ref) {
        ref->fetch_add(1);
    }
}

MemBlock::MemBlock(MemBlock &&other) noexcept : m_data(nullptr),
                                       ref(nullptr),
                                       m_length(0) {
    *this = move(other);
}

MemBlock& MemBlock::operator=(const MemBlock &other) {

    if (this != &other) {
        if (ref) {
            ref->fetch_sub(1);
            if (0 == ref->load()) {
                delete[] m_data;
                delete ref;
            }
        }

        m_data = other.m_data;
        ref = other.ref;
        m_length = other.m_length;
        if (ref) {
            ref->fetch_add(1);
        }
    }

    return *this;
}

MemBlock& MemBlock::operator=(MemBlock &&other) noexcept {
    if (this != &other) {
        if (ref) {
            ref->fetch_sub(1);
            if(0 == ref->load()) {
                delete[](m_data);
                delete ref;
                m_length = 0;
            }
        }

        // Assign
        ref = other.ref;
        m_data = other.m_data;
        m_length = other.m_length;

        // Nullify other to prevent releasing resource transferred
        other.m_data = nullptr;
        other.ref = nullptr;
        other.m_length = 0;
    }

    return *this;
}

MemBlock::~MemBlock() {
    if (ref) {
        ref->fetch_sub(1);
        if (0 == ref->load()) {
            delete[](m_data);
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
    if (offset >= queue.size()) {
        return vector<MemBlock>();
    }

    return vector<MemBlock>(queue.begin() + offset,
                            offset + number > queue.size() ? queue.end() : queue.begin() + offset + number);
}