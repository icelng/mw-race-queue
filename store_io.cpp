//
// Created by yiran on 18-7-5.
//
#include <fcntl.h>
#include <memory>
#include "unistd.h"
#include "store_io.h"
#include "sys/mman.h"
#include <iostream>
#include <cerrno>
#include <cstring>

using namespace std;

StoreIO::StoreIO(const char* file_path, u_int64_t file_size, u_int64_t region_size) {

    region_size--;
    region_bits_len = 0;
    while (region_size > 0) {
        region_bits_len++;
        region_size = region_size >> 1;
    }
    this->region_size = 1 << region_bits_len;
    this->region_mask = this->region_size - 1;
    this->regions_num = static_cast<u_int32_t>(file_size >> this->region_bits_len);
    this->is_read_mode = false;

    if (access(file_path, F_OK) != -1) {
        /*文件存在则删除*/
        cout << "Delete existed log-file" << endl;
        unlink(file_path);
    }

    cout << "Open log-file" << endl;
    file_fd = open(file_path, O_RDWR | O_CREAT);
    ftruncate(file_fd, file_size);
    regions = static_cast<void **>(malloc(sizeof(void*) * regions_num));
//    for (int i = 0;i < regions_num;i++) {
//        regions[i] = mmap(NULL, this->region_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, (long) i * this->region_size);
//        if (regions[i] == reinterpret_cast<void *>(-1)) {
//            cout << "Failed to map file!!!" << strerror(errno) << endl;
//        }
//        printf("mapped region:0x%lx, phy_address:0x%lx, region_size:%dM\n", regions[i], (long) i * this->region_size, ((this->region_size) >> 20));
//        cout << "mapped region:" << regions[i] << ", region_size:" << ((this->region_size) >> 20) << "M" << endl;
//    }
    region_ptr_now = mmap(NULL, this->region_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, file_fd, (long) 0);
    region_no_now = 0;
    printf("Mapped region_mask:0x%x\n", region_mask);

    cout << "Mapped file successfully!" << endl;

}

void* StoreIO::get_region(u_int64_t addr) {
    u_int32_t region_no = static_cast<u_int32_t>(addr >> region_bits_len);

    if (region_no > regions_num) {
        cout << "Illegal address when get_region!" << endl;
    }

    if (!is_read_mode) {
        lock_guard<std::mutex> lock(mutex);
        if (region_no != region_no_now) {
            munmap(region_ptr_now, region_size);
            region_ptr_now = mmap(NULL, region_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, file_fd, (long) region_no * region_size);
            if (region_ptr_now == reinterpret_cast<void *>(-1)) {
                cout << "Failed to map file!!!" << strerror(errno) << endl;
            }
            region_no_now = region_no;
        }

    } else {
        region_ptr_now = regions[region_no];
    }

    return region_ptr_now;
}

void StoreIO::set_read_mode() {
    if (!is_read_mode) {
        munmap(region_ptr_now, region_size);
        for (int i = 0;i < regions_num;i++) {
            regions[i] = mmap(NULL, this->region_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, file_fd, (long) i * this->region_size);
            if (regions[i] == reinterpret_cast<void *>(-1)) {
                cout << "Failed to map file!!!" << strerror(errno) << endl;
            }
            printf("mapped region:0x%lx, phy_address:0x%lx, region_size:%dM\n", regions[i], (long) i * this->region_size, ((this->region_size) >> 20));
        }
    }
    is_read_mode = true;
}
