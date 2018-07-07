//
// Created by yiran on 18-7-5.
//

#include "message_queue.h"
#include "store_io.h"
#include "idle_page_manager.h"
#include "commit_service.h"
#include "buffer_pool.h"
#include "malloc.h"

using namespace std;
using namespace race2018;

#define MSG_HEAD_SIZE 2
#define INITIAL_PAGE_TABLE_LEN 64
#define EXPEND_PAGE_TABLE_LEN 8

MessageQueue::MessageQueue(IdlePageManager *idle_page_manager, StoreIO *store_io, CommitService *commit_service, BufferPool *buffer_pool) {

    this->idle_page_manager = idle_page_manager;
    this->store_io = store_io;
    this->commit_service = commit_service;
    this->buffer_pool = buffer_pool;
    put_buffer_offset = 0;
    buffer_size = buffer_pool->get_buffer_size();
    put_buffer = nullptr;
    is_need_commit = false;
    queue_len = 0;
    last_page_index = 0;
    need_commit_size = 0;
    page_table = static_cast<u_int64_t *>(malloc(sizeof(u_int64_t) * (INITIAL_PAGE_TABLE_LEN * 2)));
    page_table_len = INITIAL_PAGE_TABLE_LEN;
    max_commit_q_len = (idle_page_manager->get_page_size() / buffer_size) * 4 + 1;
    commit_buffer_queue = (void **) malloc(max_commit_q_len * sizeof(void*));
    commit_q_head = 0;
    commit_q_tail = 0;

    sem_init(&commit_sem_lock, 0, 1);
}

void MessageQueue::put(const race2018::MemBlock &mem_block) {
    lock_guard<mutex> lock(mtx);

    is_need_commit = true;
    if (put_buffer == nullptr) {
        put_buffer = buffer_pool->borrow_buffer();
        put_buffer_offset = 0;
    }

    /*判断是否攒够一页, 如果是, 则commit*/
    if ((need_commit_size + mem_block.size + MSG_HEAD_SIZE) >= idle_page_manager->get_page_size()) {

        if (last_page_index + 1 >= page_table_len) {
            expend_page_table();
        }

        commit_later();
//        commit_now();
//        cout << "commit buffer queue size:" << commit_buffer_queue.unsafe_size() << endl;
//        cout << "hold buffers num:" << hold_buffers_num << endl;
        put_buffer = buffer_pool->borrow_buffer();
        put_buffer_offset = 0;
        is_need_commit = true;
        hold_buffers_num++;

        last_page_index++;
        page_table[last_page_index * 2 + 1] = queue_len;

    }

    unsigned char msg_head[MSG_HEAD_SIZE];
    u_int32_t put_offset = 0;
    size_t seg_save_size;
    bool is_head = true;
    shortToBytes(static_cast<unsigned short>(mem_block.size), msg_head, 0);
//    printf("head[0]:0x%x, head[1]:0x%x, size:%d\n", msg_head[0], msg_head[1], static_cast<unsigned short>(mem_block.size));
    while (put_offset < (mem_block.size + MSG_HEAD_SIZE)) {
        seg_save_size = std::min(buffer_size - put_buffer_offset,
                                       (mem_block.size + MSG_HEAD_SIZE) - put_offset);
//        cout << "seg_save_size:" << seg_save_size << endl;
        /*先写头*/
        if (is_head) {
            size_t put_size = std::min(MSG_HEAD_SIZE - put_offset, static_cast<const u_int32_t &>(seg_save_size));
            memcpy(put_buffer + put_buffer_offset, msg_head + put_offset, put_size);
            put_buffer_offset += put_size;
            put_offset += put_size;
            if (put_offset == MSG_HEAD_SIZE) {
                is_head = false;
            }
        } else {
            memcpy(put_buffer + put_buffer_offset, mem_block.ptr + (put_offset - MSG_HEAD_SIZE), seg_save_size);
            put_buffer_offset += seg_save_size;
            put_offset += seg_save_size;
        }

        if (put_buffer_offset == buffer_size) {
            commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
//            cout << "borrow buffer:" << commit_buffer_queue.unsafe_size() << endl;
            put_buffer = buffer_pool->borrow_buffer();
            put_buffer_offset = 0;
        }
    }

    page_table[last_page_index * 2 + 1] = ++queue_len;
    need_commit_size += (MSG_HEAD_SIZE + mem_block.size);

    free(mem_block.ptr);

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

//    cout << "commit now" << endl;
    commit_now();

    u_int32_t cur_page_index = find_page_index(start_msg_index);

    for (int i = 0; i < adjust_num;i += read_num) {

        u_int64_t msg_page_phy_address = page_table[cur_page_index * 2];
        read_num = std::min(page_table[cur_page_index * 2 + 1] - (start_msg_index + i), adjust_num - i);
//        if (read_num <= 0) {
//            cout << "Read num = 0?ha? adjust_num:" << adjust_num << ", i:" << i << endl;
//        }
        void* mapped_region_ptr = store_io->get_region(msg_page_phy_address);
        void* page_start_ptr = mapped_region_ptr + (msg_page_phy_address & store_io->region_mask);
        u_int64_t start_index_in_page = cur_page_index == 0 ?
                start_msg_index : (start_msg_index + i) - page_table[(cur_page_index - 1) * 2 + 1];
        u_int64_t offset_in_page = locate_msg_offset_in_page(page_start_ptr, start_index_in_page);
        void* read_start_ptr = page_start_ptr + offset_in_page;

        u_int64_t read_offset = 0;
        for (int j = 0;j < read_num;j++) {
            race2018::MemBlock block;
            auto msg_size = bytesToShort(static_cast<unsigned char *>(read_start_ptr + read_offset), 0);
            read_offset += MSG_HEAD_SIZE;
            void* msg_buf = malloc(msg_size);
            memset(msg_buf, 0, msg_size);
            memcpy(msg_buf, read_start_ptr + read_offset, msg_size);
            int index = start_msg_index + i + j;
            int msg_len = 1;
            index = index / 10;
            while (index != 0) {
                msg_len++;
                index = index / 10;
            }
            index = start_msg_index + i + j;
            char save = ((char*)msg_buf)[msg_len];
            ((char *)msg_buf)[msg_len] = 0;
            if (strcmp(std::to_string(index).c_str(), static_cast<const char *>(msg_buf)) != 0) {
                printf("error, size:%d, msg:%s, need:%d, address:0x%lx, page start address:0x%lx start_index_in_page:%ld, offset_in_page:%ld\n", msg_size, (char*) msg_buf, index, msg_page_phy_address + offset_in_page + read_offset, msg_page_phy_address, start_index_in_page, offset_in_page);
                for (int k = 0;k < 4096;k++) {
                    printf("%x ", ((unsigned char *)page_start_ptr)[k]);
                    if ((k + 1) % 32 == 0) {
                        printf("\n");
                    }
                }
                printf("\n");
//                cout << "error msg size:" << msg_size << ", msg:" << (char *) msg_buf << ", need:" << index << ", phy address:" << msg_page_phy_address + offset_in_page << endl;
//                std::cout << "Check error:" << (char *)msg_buf << ", need:"<< index <<  std::endl;
            }
            ((char *)msg_buf)[msg_len] = save;
//            cout << "Get msg:" << (char *)msg_buf << endl;
//            if (strcmp(std::to_string(start_msg_index + i + j).c_str(), static_cast<const char *>(block.ptr)) != 0) {
//                cout << "error msg size:" << msg_size << ", msg:" << (char *) msg_buf;
//                cout << "phy address:" << msg_page_phy_address << endl;
//            }
//            if (msg_size != 51) {
//                cout << "error msg size:" << msg_size << ", msg:" << (char *) msg_buf;
//                cout << "phy address:" << msg_page_phy_address << endl;
//            }
            read_offset += msg_size;
            block.size = msg_size;
            block.ptr = msg_buf;
            ret.push_back(block);
        }

        cur_page_index ++;
    }

    return ret;
}

/**
 * 不能被直接调用
 * */
void MessageQueue::do_commit() {
    u_int64_t idle_page_phy_address;
    void* idle_page_mem_ptr;
    void* mapped_region_ptr;


    idle_page_phy_address = idle_page_manager->get_one_page();
    mapped_region_ptr = store_io->get_region(idle_page_phy_address);
    idle_page_mem_ptr = mapped_region_ptr + (idle_page_phy_address & store_io->region_mask);

    //cout << "idle_page_phy_address:" << idle_page_phy_address << ", mapped_region_ptr:" << mapped_region_ptr << ", idle_page_mem_ptr:" << idle_page_mem_ptr << endl;

    u_int64_t commit_offset = 0;
    int i = 0;
    while (commit_offset < committing_size) {
        void* commit_buffer;
//        if (!commit_buffer_queue.try_pop(commit_buffer)) {
//            cout << "Failed to pop commit_buffer when doing commit!!" << endl;
//            break;
//        }
        commit_buffer = commit_buffer_queue[commit_q_head++%max_commit_q_len];
        size_t commit_size = std::min(committing_size - commit_offset, buffer_size);
//        cout << "commit size:" << commit_size << "committing size:" << committing_size << endl;
        memcpy(idle_page_mem_ptr + commit_offset, commit_buffer, commit_size);
        buffer_pool->return_buffer(commit_buffer);
        commit_offset += commit_size;
        if (commit_offset > idle_page_manager->get_page_size()) {
            printf("The commit_offset(%ld) is larger than 4096", commit_offset);
        }
        if (i++ >= idle_page_manager->get_page_size()/buffer_size) {
            cout << "commit buffers error!!!num:" << i << endl;
        }
    }
    hold_buffers_num--;

    page_table[committing_page_index * 2] = idle_page_phy_address;

//    cout << "committed successfully!" << endl;
    sem_post(&commit_sem_lock);
}

void MessageQueue::commit_later() {

    if (!is_need_commit) {
        return;
    }

    sem_wait(&commit_sem_lock);
    commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
//    commit_buffer_queue.push(put_buffer);
//    cout << "commit later" << endl;
    put_buffer = nullptr;
    committing_size = need_commit_size;
    need_commit_size = 0;
    committing_page_index = last_page_index;
    commit_service->request_commit(this);
    is_need_commit = false;

}

void MessageQueue::commit_now() {

    if (!is_need_commit) {
        return;
    }

    sem_wait(&commit_sem_lock);
    commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
//    commit_buffer_queue.push(put_buffer);
//    cout << "commit now" << endl;
    put_buffer = nullptr;
    committing_size = need_commit_size;
    need_commit_size = 0;
    committing_page_index = last_page_index;
    do_commit();
    is_need_commit = false;

}

void MessageQueue::shortToBytes(unsigned short v, unsigned char *b, int off) {

    b[off + 1] = (unsigned char) v;
    b[off + 0] = (unsigned char) (v >> 8);

}

unsigned short MessageQueue::bytesToShort(unsigned char *b, int off) {
    return (unsigned short) (((b[off + 1] & 0xFFL) << 0) | ((b[off + 0] & 0xFFL) << 8));
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
        unsigned short msg_size = bytesToShort((unsigned char *) (page_start_ptr + offset), 0);
        if (msg_size > 83) {
            printf("failed locate msg, msg_size:%d, i:%d, offset:%ld\n", msg_size, i, offset);
//            cout << "failed locate msg, msg_size:" << msg_size << ", offset:" << offset << endl;
//            for (int j = 0;j < 4096;j++) {
//                printf("%x ", ((unsigned char *)page_start_ptr)[j]);
//                if (j % 32 == 0) {
//                    printf("\n");
//                }
//            }
//            printf("\n");
        }
        offset += (msg_size + MSG_HEAD_SIZE);
    }

    return offset;
}

u_int32_t MessageQueue::find_page_index(u_int64_t msg_index) {
    u_int32_t top = last_page_index;
    u_int32_t bottom = 0;
    u_int32_t mid;

    for (mid = (top + bottom) / 2;bottom < top;mid = (top + bottom) / 2) {
        if (page_table[mid * 2 + 1] <= msg_index) {
            bottom = mid + 1;
        } else {
            top = mid;
        }
    }

    return mid;
}


