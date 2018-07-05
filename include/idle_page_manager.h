//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_IDLE_PAGE_MANAGER_H
#define QUEUE_RACE_IDLE_PAGE_MANAGER_H

#define PAGE_SIZE_4K 4096

class IdlePageManager {
public:
    IdlePageManager(u_int64_t total_size, u_int32_t page_size);
    u_int64_t get_one_page();
    u_int32_t get_page_size();

private:
    u_int64_t total_page_num;
    u_int32_t page_bits_len;
    u_int32_t page_size;
    std::atomic<u_int64_t> idle_page_start_index;

};

#endif //QUEUE_RACE_IDLE_PAGE_MANAGER_H
