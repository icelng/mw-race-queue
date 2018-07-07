//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_STORE_IO_H
#define QUEUE_RACE_STORE_IO_H


#include <mutex>

class StoreIO {
public:
    StoreIO(const char *file_path, u_int64_t file_size, u_int64_t region_size);

    void* get_region(u_int64_t addr);
    void set_read_mode();
    u_int32_t region_mask;

private:
    void** regions;
    u_int32_t region_bits_len;
    u_int32_t region_size;
    u_int32_t regions_num;
    u_int32_t region_no_now;
    int file_fd;
    void* region_ptr_now;
    std::mutex mutex;
    bool is_read_mode;
};

#endif //QUEUE_RACE_STORE_IO_H
