//
// Created by yiran on 18-7-5.
//

#ifndef QUEUE_RACE_STORE_IO_H
#define QUEUE_RACE_STORE_IO_H



class StoreIO {
public:
    StoreIO(const char *file_path, u_int64_t file_size, u_int32_t region_size);

    void* get_region(u_int64_t addr);
    u_int32_t region_mask;

private:
    void** regions;
    u_int32_t region_bits_len;
    u_int32_t region_size;
    u_int32_t regions_num;
};

#endif //QUEUE_RACE_STORE_IO_H
