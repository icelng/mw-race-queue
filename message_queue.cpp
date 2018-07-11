//
// Created by yiran on 18-7-5.
//

#include "message_queue.h"
#include "store_io.h"
#include "idle_page_manager.h"
#include "commit_service.h"
#include "buffer_pool.h"
#include "malloc.h"
#include "read_ahead_service.h"

using namespace std;
using namespace race2018;

#define MSG_HEAD_SIZE 2
#define INITIAL_PAGE_TABLE_LEN 40
#define EXPEND_PAGE_TABLE_LEN 8
#define MAX_READ_CACHE_QUEUE_LEN 2

MessageQueue::MessageQueue(IdlePageManager *idle_page_manager,
                           StoreIO *store_io,
                           CommitService *commit_service,
                           ReadAheadService *read_ahead_service,
                           BufferPool *buffer_pool) {

    this->idle_page_manager = idle_page_manager;
    this->store_io = store_io;
    this->commit_service = commit_service;
    this->buffer_pool = buffer_pool;
    this->read_ahead_service = read_ahead_service;
    put_buffer_offset = 0;
    buffer_size = buffer_pool->get_buffer_size();
    page_size = idle_page_manager->get_page_size();
    buffers_num_per_page = page_size / buffer_size;
    put_buffer = nullptr;
    is_need_commit = false;
    queue_len = 0;
    last_page_index = 0;
    need_commit_size = 0;
    page_table = (PageEntry*) (malloc(sizeof(PageEntry) * INITIAL_PAGE_TABLE_LEN));
    page_table_len = INITIAL_PAGE_TABLE_LEN;
    max_commit_q_len = (page_size / buffer_size) * 2 + 1;
    commit_buffer_queue = (void **) malloc(max_commit_q_len * sizeof(void*));
    commit_q_head = 0;
    commit_q_tail = 0;

    /**read cache**/
    is_read_cache_actived = false;
    read_cache_trigger = 0;
    max_rc_q_len = MAX_READ_CACHE_QUEUE_LEN;
    read_cache_queue = (ReadCache*) malloc(sizeof(ReadCache) * (max_rc_q_len + 1));
    read_cache_num = 0;
    rc_q_head = 0;
    rc_q_tail = 0;
    last_read_index = -1;
    last_read_offset_in_page = 0;
    pthread_mutex_init(&read_cache_q_lock, 0);
    sem_init(&read_ahead_sem_lock, 0, 1);
    is_reading = false;

    commit_service->set_need_commit(this);

    sem_init(&commit_sem_lock, 0, 1);
}

void MessageQueue::put(const race2018::MemBlock &mem_block) {
//    lock_guard<mutex> lock(mtx);

    is_need_commit = true;
    if (put_buffer == nullptr) {
        put_buffer = buffer_pool->borrow_buffer();
        put_buffer_offset = 0;
    }
    size_t add_size = mem_block.size + MSG_HEAD_SIZE;

    /*判断是否攒够一页, 如果是, 则commit*/
    if ((need_commit_size + add_size) >= page_size) {

        if (last_page_index + 1 >= page_table_len) {
            expend_page_table();
        }

        commit_later();
//        commit_now();
        put_buffer = buffer_pool->borrow_buffer();
        put_buffer_offset = 0;
        is_need_commit = true;

        last_page_index++;
        page_table[last_page_index].queue_len = queue_len;

    }

    accumulate_to_buffers(mem_block);

    page_table[last_page_index].queue_len = ++queue_len;
    need_commit_size += add_size;

    if (need_commit_size > page_size - 30) {
        if (last_page_index + 1 >= page_table_len) {
            expend_page_table();
        }

        commit_later();
        put_buffer_offset = 0;
        is_need_commit = false;

        last_page_index++;
        page_table[last_page_index].queue_len = queue_len;

    }

}

inline void MessageQueue::accumulate_to_buffers(const MemBlock &mem_block) {
    unsigned char msg_head[MSG_HEAD_SIZE];
    u_int32_t put_offset = 0;
    size_t seg_save_size;
    bool is_head = true;
    shortToBytes(static_cast<unsigned short>(mem_block.size), msg_head, 0);
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
            seg_save_size -= put_size;
            if (put_offset == MSG_HEAD_SIZE) {
                is_head = false;
            }
        }

        if (!is_head) {
            memcpy(put_buffer + put_buffer_offset, mem_block.ptr + (put_offset - MSG_HEAD_SIZE), seg_save_size);
            put_buffer_offset += seg_save_size;
            put_offset += seg_save_size;
        }

        if (put_buffer_offset == buffer_size) {
            commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
            put_buffer = buffer_pool->borrow_buffer();
            put_buffer_offset = 0;
        }
    }

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
//    commit_now();
    commit_service->commit_all();

    if (read_cache_trigger++ > 3) {
        is_read_cache_actived = true;
    }

    u_int32_t cur_page_index = find_page_index(start_msg_index);
    u_int64_t msg_page_phy_address = 0;

    for (int i = 0; i < adjust_num;i += read_num) {

        msg_page_phy_address = page_table[cur_page_index].addr;
        read_num = std::min(page_table[cur_page_index].queue_len - (start_msg_index + i), adjust_num - i);

        u_int64_t read_offset = 0;
        void* read_start_ptr = 0;
        u_int64_t offset_in_page;
        u_int64_t start_index_in_page;
        void* page_start_ptr;
        bool is_hit_read_cache = false;

        if (is_read_cache_actived && ((read_cache_num != 0) || is_reading)) {
            while (true) {
                if (read_cache_num == 0 && is_reading) {
                    sem_wait(&read_ahead_sem_lock);
                    sem_post(&read_ahead_sem_lock);
                }

                pthread_mutex_lock(&read_cache_q_lock);
                long num = read_cache_num;
                u_int64_t head_start = rc_q_head;
                for (int j = 0;j < num;j++) {
                    ReadCache *read_cache = &read_cache_queue[(head_start + j) % (max_rc_q_len + 1)];
                    if (read_cache->phy_address == msg_page_phy_address) {
                        /*命中读缓存*/
                        is_hit_read_cache = true;
                        page_start_ptr = read_cache->page_cache;
                        if (last_read_index == start_msg_index && last_read_page_address == msg_page_phy_address) {
                            offset_in_page = last_read_offset_in_page;
                        } else {
                            start_index_in_page = cur_page_index == 0 ?
                                                  start_msg_index : (start_msg_index + i) - page_table[cur_page_index - 1].queue_len;
                            offset_in_page = locate_msg_offset_in_page(read_cache->page_cache, start_index_in_page);
                        }
                        read_start_ptr = read_cache->page_cache + offset_in_page;
                        break;
                    } else {
                        /*没有命中,丢弃*/
                        rc_q_head++;
                        read_cache_num--;
                        free(read_cache->page_cache);
                    }
                }
                pthread_mutex_unlock(&read_cache_q_lock);

                if (is_hit_read_cache || (read_cache_num == 0 && !is_reading)) {
                    break;
                }
            }
        }

        if (!is_hit_read_cache) {
            /*若没有命中缓存页,则使用map映射页*/
            void* mapped_region_ptr = store_io->get_region(msg_page_phy_address);
            page_start_ptr = mapped_region_ptr + (msg_page_phy_address & store_io->region_mask);
            start_index_in_page = cur_page_index == 0 ?
                                            start_msg_index : (start_msg_index + i) - page_table[cur_page_index - 1].queue_len;
            offset_in_page = locate_msg_offset_in_page(page_start_ptr, start_index_in_page);
            read_start_ptr = page_start_ptr + offset_in_page;
        }

        /**已获取到数据页,并且定位到消息的页偏移,开始读数据**/
        for (int j = 0;j < read_num;j++) {
            race2018::MemBlock block;
            auto msg_size = bytesToShort(static_cast<unsigned char *>(read_start_ptr + read_offset), 0);
            read_offset += MSG_HEAD_SIZE;
            void* msg_buf = malloc(msg_size);
            memcpy(msg_buf, read_start_ptr + read_offset, msg_size);

            /**调试代码**/
//            int index = start_msg_index + i + j;
//            int msg_len = 1;
//            index = index / 10;
//            while (index != 0) {
//                msg_len++;
//                index = index / 10;
//            }
//            index = start_msg_index + i + j;
//            char save = ((char*)msg_buf)[msg_len];
//            ((char *)msg_buf)[msg_len] = 0;
//            if (strcmp(std::to_string(index).c_str(), static_cast<const char *>(msg_buf)) != 0) {
//                printf("error, is_hit_read_cache:%d, size:%d, msg:%s, need:%d, address:0x%lx, page start address:0x%lx start_index_in_page:%ld, offset_in_page:%ld\n", is_hit_read_cache, msg_size, (char*) msg_buf, index, msg_page_phy_address + offset_in_page + read_offset, msg_page_phy_address, start_index_in_page, offset_in_page);
//                for (int k = 0;k < 4096;k++) {
//                    printf("%x ", ((unsigned char *)page_start_ptr)[k]);
//                    if ((k + 1) % 32 == 0) {
//                        printf("\n");
//                    }
//                }
//                printf("\n");
//                cout << "error msg size:" << msg_size << ", msg:" << (char *) msg_buf << ", need:" << index << ", phy address:" << msg_page_phy_address + offset_in_page << endl;
//                std::cout << "Check error:" << (char *)msg_buf << ", need:"<< index <<  std::endl;
//            }
//            ((char *)msg_buf)[msg_len] = save;


            read_offset += msg_size;
            block.size = msg_size;
            block.ptr = msg_buf;
            ret.push_back(block);
            last_read_page_address = msg_page_phy_address;
            last_read_offset_in_page = offset_in_page + read_offset;
            last_read_index = start_msg_index + i + j + 1;
        }

        cur_page_index++;
    }


    /**判断是否需要发起异步读请求**/
    if (is_read_cache_actived && read_cache_num <= 1) {
        sem_wait(&read_ahead_sem_lock);  // 等待正在进行的异步请求完毕(如果有)

        if (read_cache_num > 1) {
            /*不需要发起预读*/
            sem_post(&read_ahead_sem_lock);
            return ret;
        }

        if (read_cache_num == 0) {
            /*此时,本次读请求最后读到的页一定是mapped页,把其加入到cache队列中*/
            void* mapped_region_ptr = store_io->get_region(msg_page_phy_address);
            void* page_start_ptr = mapped_region_ptr + (msg_page_phy_address & store_io->region_mask);
            void* page_cache = malloc(page_size);
            memcpy(page_cache, page_start_ptr, page_size);
            pthread_mutex_lock(&read_cache_q_lock);
            read_cache_queue[rc_q_tail % (max_rc_q_len + 1)].page_cache = page_cache;
            read_cache_queue[rc_q_tail++ % (max_rc_q_len + 1)].phy_address = msg_page_phy_address;
            read_cache_num++;
            pthread_mutex_unlock(&read_cache_q_lock);
        }

        /*发起下一页开始的异步读请求*/
        ra_start_page_index = cur_page_index;
        is_reading = true;
        read_ahead_service->request_read_ahead(this);
    }

    return ret;
}

/**
 * 不能被直接调用
 * */
void MessageQueue::do_commit() {
    u_int64_t idle_page_phy_address;

    idle_page_phy_address = idle_page_manager->get_one_page();

    u_int64_t commit_offset = 0;
    int i = 0;
    pthread_mutex_lock(&commit_service->commit_mutex);   // 并发commit page会乱
    while (commit_offset < committing_size) {
        void* commit_buffer;
        commit_buffer = commit_buffer_queue[commit_q_head++%max_commit_q_len];
        size_t commit_size = std::min(committing_size - commit_offset, buffer_size);
//        cout << "commit size:" << commit_size << "committing size:" << committing_size << endl;
        store_io->write_data(commit_buffer, buffer_size);
        buffer_pool->return_buffer(commit_buffer);
        commit_offset += commit_size;
        if (commit_offset > page_size) {
            printf("The commit_offset(%ld) is larger than 4096", commit_offset);
        }
        if (i++ >= buffers_num_per_page) {
            cout << "commit buffers error!!!num:" << i << endl;
        }
    }

    while (i++ < buffers_num_per_page) {
        /*补充一页*/
        store_io->add_offset(buffer_size);
    }
    pthread_mutex_unlock(&commit_service->commit_mutex);

    page_table[committing_page_index].addr = idle_page_phy_address;

    sem_post(&commit_sem_lock);
}

inline void MessageQueue::commit_later() {

    if (!is_need_commit) {
        return;
    }

    sem_wait(&commit_sem_lock);

    if (!is_need_commit) {
        return;
    }

    commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
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

    if (!is_need_commit) {
        return;
    }

    commit_buffer_queue[commit_q_tail++%max_commit_q_len] = put_buffer;
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

    PageEntry* new_page_table = (PageEntry*) (malloc(sizeof(PageEntry) * (page_table_len + EXPEND_PAGE_TABLE_LEN)));
    for (int i = 0;i < page_table_len * 2;i++) {
        new_page_table[i].queue_len = page_table[i].queue_len;
        new_page_table[i].addr = page_table[i].addr;
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
        offset += (msg_size + MSG_HEAD_SIZE);
    }

    return offset;
}

u_int32_t MessageQueue::find_page_index(u_int64_t msg_index) {
    u_int32_t top = last_page_index;
    u_int32_t bottom = 0;
    u_int32_t mid;

    for (mid = (top + bottom) / 2;bottom < top;mid = (top + bottom) / 2) {
        if (page_table[mid].queue_len <= msg_index) {
            bottom = mid + 1;
        } else {
            top = mid;
        }
    }

    return mid;
}


void MessageQueue::do_read_ahead() {
    size_t have_read_page_num = 0;
    size_t need_read_page_num = 0;
    u_int64_t msg_page_phy_address;
    void* mapped_region_ptr;
    void* page_start_ptr;

    need_read_page_num = std::min(max_rc_q_len - read_cache_num, max_rc_q_len - have_read_page_num);
    while (need_read_page_num != 0) {

        for (int i = 0;i < need_read_page_num;i++) {
            void* page_cache = malloc(page_size);
            msg_page_phy_address = page_table[ra_start_page_index + have_read_page_num].addr;
            mapped_region_ptr = store_io->get_region(msg_page_phy_address);
            page_start_ptr = mapped_region_ptr + (msg_page_phy_address & store_io->region_mask);
            memcpy(page_cache, page_start_ptr, page_size);

            pthread_mutex_lock(&read_cache_q_lock);
            read_cache_queue[rc_q_tail % (max_rc_q_len + 1)].page_cache = page_cache;
            read_cache_queue[rc_q_tail++ % (max_rc_q_len + 1)].phy_address = msg_page_phy_address;
            read_cache_num++;
            pthread_mutex_unlock(&read_cache_q_lock);

            have_read_page_num++;
        }

        need_read_page_num = std::min(max_rc_q_len - read_cache_num, max_rc_q_len - have_read_page_num);
    }

    is_reading = false;
    sem_post(&read_ahead_sem_lock);

}




