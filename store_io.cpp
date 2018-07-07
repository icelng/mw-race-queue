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
    int fd = 0;

    region_size--;
    region_bits_len = 0;
    while (region_size > 0) {
        region_bits_len++;
        region_size = region_size >> 1;
    }
    this->region_size = 1 << region_bits_len;
    this->region_mask = this->region_size - 1;
    this->regions_num = static_cast<u_int32_t>(file_size >> this->region_bits_len);

    if (access(file_path, F_OK) != -1) {
        /*文件存在则删除*/
        cout << "Delete existed log-file" << endl;
        unlink(file_path);
    }

    cout << "Open log-file" << endl;
    fd = open(file_path, O_RDWR | O_CREAT);
    ftruncate(fd, file_size);
    regions = static_cast<void **>(malloc(sizeof(void*) * regions_num));
    for (int i = 0;i < regions_num;i++) {
        regions[i] = mmap(NULL, this->region_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, (long) i * this->region_size);
        if (regions[i] == reinterpret_cast<void *>(-1)) {
            cout << "Failed to map file!!!" << strerror(errno) << endl;
        }
        printf("mapped region:0x%lx, phy_address:0x%lx, region_size:%dM\n", regions[i], (long) i * this->region_size, ((this->region_size) >> 20));
//        cout << "mapped region:" << regions[i] << ", region_size:" << ((this->region_size) >> 20) << "M" << endl;
    }
    printf("Mapped region_mask:0x%x\n", region_mask);

    cout << "Mapped file successfully!" << endl;

}

void* StoreIO::get_region(u_int64_t addr) {
    u_int32_t region_no = static_cast<u_int32_t>(addr >> region_bits_len);

    if (region_no > regions_num) {
        cout << "Illegal address when get_region!" << endl;
    }

    return regions[region_no];
}
