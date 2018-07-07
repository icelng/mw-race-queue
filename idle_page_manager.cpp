//
// Created by yiran on 18-7-5.
//

#include <iostream>
#include <atomic>
#include "idle_page_manager.h"

using namespace std;

IdlePageManager::IdlePageManager(u_int64_t total_size, u_int32_t page_size) {

//    if (page_size > total_size || page_size < PAGE_SIZE_4K) {
//        page_size = PAGE_SIZE_4K;
//    }

    page_bits_len = 0;
    page_size--;
    while (page_size != 0) {
        page_bits_len++;
        page_size = page_size >> 1;
    }

    total_page_num = total_size >> page_bits_len;
    this->page_size = static_cast<u_int32_t>(1 << page_bits_len);
    this->idle_page_start_index = 0;

    cout << "Idle total size:" << total_size << ", page bits len:" << page_bits_len << ", page size:"
         << this->page_size << endl;

}

u_int64_t IdlePageManager::get_one_page() {

    u_int64_t page_index = idle_page_start_index.fetch_add(1);
    if (page_index > total_page_num) {
        cout << "There is not enough idle pages, totalPageNum:%d" << endl;
    }

    return page_index << page_bits_len;
}

u_int32_t IdlePageManager::get_page_size() {
    return this->page_size;
}


