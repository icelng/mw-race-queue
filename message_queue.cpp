//
// Created by yiran on 18-7-5.
//

#include "message_queue.h"
#include "store_io.h"
#include "idle_page_manager.h"
#include "commit_service.h"

using namespace std;
using namespace race2018;

#define MSG_HEAD_SIZE 2
#define INITIAL_PAGE_TABLE_LEN 32
#define EXPEND_PAGE_TABLE_LEN 8

MessageQueue::MessageQueue(IdlePageManager *idle_page_manager, StoreIO *store_io, CommitService *commit_service) {

    this->idle_page_manager = idle_page_manager;
    this->store_io = store_io;
    this->commit_service = commit_service;
    is_need_commit = false;
    queue_len = 0;
    last_page_index = 0;
    need_commit_size = 0;
    page_table = static_cast<u_int64_t *>(malloc(sizeof(u_int64_t) * (INITIAL_PAGE_TABLE_LEN * 2)));
    page_table_len = INITIAL_PAGE_TABLE_LEN;

    sem_init(&commit_sem_lock, 0, 1);
}

void MessageQueue::put(const race2018::MemBlock &mem_block) {
    lock_guard<mutex> lock(mtx);

    is_need_commit = true;

    /*判断是否攒够一页, 如果是, 则commit*/
    if (need_commit_size + mem_block.size + MSG_HEAD_SIZE> idle_page_manager->get_page_size()) {

        if (last_page_index + 1 >= page_table_len) {
            expend_page_table();
        }

        commit_later();
        last_page_index++;
        need_commit_size = 0;

    }

    msg_put_queue.push(mem_block);
    page_table[last_page_index * 2 + 1] = ++queue_len;
    need_commit_size += (MSG_HEAD_SIZE + mem_block.size);

}

std::vector<race2018::MemBlock> MessageQueue::get(long start_msg_index, long msg_num) {
    lock_guard<mutex> lock(mtx);

    vector<race2018::MemBlock> ret;
    u_int64_t adjust_num = start_msg_index + msg_num > queue_len ?
            queue_len - start_msg_index : msg_num;
    u_int64_t read_num;

    if (start_msg_index > queue_len || start_msg_index < 0) {
        return ret;
    }

    commit_now();

    u_int32_t cur_page_index = find_page_index(start_msg_index);

    for (int i = 0; i < adjust_num;i += read_num) {

        u_int64_t msg_page_phy_address = page_table[cur_page_index * 2];
        read_num = std::min(page_table[cur_page_index * 2 + 1] - (start_msg_index + i), adjust_num - i);
        void* mapped_region_ptr = store_io->get_region(msg_page_phy_address);
        void* page_start_ptr = mapped_region_ptr + (msg_page_phy_address & store_io->region_mask);
        u_int64_t start_index_in_page = cur_page_index == 0 ?
                start_msg_index : (start_msg_index + i) - page_table[(cur_page_index - 1) * 2 + 1];
        void* read_start_ptr = page_start_ptr + locate_msg_offset_in_page(page_start_ptr, start_index_in_page);

        u_int64_t read_offset = 0;
        for (int j = 0;j < read_num;j++) {
            race2018::MemBlock block;
            auto msg_size = bytesToShort(static_cast<unsigned char *>(read_start_ptr + read_offset), 0);
            read_offset += MSG_HEAD_SIZE;
            void* msg_buf = malloc(msg_size);
            memcpy(msg_buf, read_start_ptr + read_offset, msg_size);
            block.size = msg_size;
            block.ptr = msg_buf;
            ret.push_back(block);
        }

    }

    return ret;
}

/**
 * 不能被直接调用
 * */
void MessageQueue::do_commit() {
    unsigned char msg_head[2];
    race2018::MemBlock mem_block;
    u_int64_t idle_page_phy_address;
    u_int64_t write_offset;
    void* idle_page_mem_ptr;
    void* mapped_region_ptr;

    idle_page_phy_address = idle_page_manager->get_one_page();
    mapped_region_ptr = store_io->get_region(idle_page_phy_address);
    idle_page_mem_ptr = mapped_region_ptr + (idle_page_phy_address & store_io->region_mask);
    write_offset = 0;

    for (int i = 0;i < commit_msg_num;i++) {

        if (!msg_put_queue.try_pop(mem_block)) {
            cout << "Failed to pop mem_block when doing commit!!" << endl;
            continue;
        }

        size_t msg_size = mem_block.size;
        shortToBytes(static_cast<unsigned short>(msg_size), msg_head, 0);
        memcpy(idle_page_mem_ptr + write_offset, msg_head, MSG_HEAD_SIZE);
        write_offset += MSG_HEAD_SIZE;
        memcpy(idle_page_mem_ptr + write_offset, mem_block.ptr, msg_size);
        write_offset += msg_size;

        free(mem_block.ptr);

    }

    page_table[committing_page_index * 2] = idle_page_phy_address;

    sem_post(&commit_sem_lock);
}

void MessageQueue::commit_later() {

    if (!is_need_commit) {
        return;
    }

    sem_wait(&commit_sem_lock);
    commit_msg_num = msg_put_queue.unsafe_size();
    committing_page_index = last_page_index;
    commit_service->request_commit(this);
    is_need_commit = false;

}

void MessageQueue::commit_now() {

    if (!is_need_commit) {
        return;
    }

    sem_wait(&commit_sem_lock);
    commit_msg_num = msg_put_queue.unsafe_size();
    committing_page_index = last_page_index;
    do_commit();
    is_need_commit = false;

}

void MessageQueue::shortToBytes(unsigned short v, unsigned char *b, int off) {

    b[off + 1] = (unsigned char) v;
    b[off + 0] = (unsigned char) (v >> 8);

}

unsigned short MessageQueue::bytesToShort(unsigned char *b, int off) {
    return (unsigned short) (((b[off + 1] & 0xFF) << 0) + ((b[off + 0] & 0xFF) << 8));
}

void MessageQueue::expend_page_table() {
    sem_wait(&commit_sem_lock);

    u_int64_t* new_page_table = static_cast<u_int64_t *>(malloc(sizeof(u_int64_t) * ((page_table_len + EXPEND_PAGE_TABLE_LEN) * 2)));
    for (int i = 0;i < page_table_len * 2;i++) {
        new_page_table[i] = page_table[i];
    }
    free(page_table);
    page_table = new_page_table;
    page_table_len += EXPEND_PAGE_TABLE_LEN;

    sem_post(&commit_sem_lock);
}

u_int64_t MessageQueue::locate_msg_offset_in_page(void *page_start_ptr, u_int64_t msg_no) {
    u_int64_t  offset = 0;

    for (int i = 0;i < msg_no;i++) {
        offset += (bytesToShort(static_cast<unsigned char *>(page_start_ptr + offset), 0) + MSG_HEAD_SIZE);
    }

    return offset;
}

u_int32_t MessageQueue::find_page_index(u_int64_t msg_index) {
    u_int32_t top = last_page_index;
    u_int32_t bottom = 0;
    u_int32_t mid;

    for (mid = (top + bottom) / 2;bottom < top;mid = (top + bottom) / 2) {
        if (page_table[mid * 2 + 1] < msg_index) {
            bottom = mid + 1;
        } else {
            top = mid;
        }
    }

    return mid;
}


