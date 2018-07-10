//
// Created by yiran on 18-7-10.
//

#include "unistd.h"
#include "memory.h"
#include "fcntl.h"
#include "sys/mman.h"
#include <cstdlib>
#include <iostream>
#include "sys/time.h"

using namespace std;

int main1() {
    struct timeval start_tv, stop_tv;
    u_int64_t start_time, stop_time;
    int file_fd;
    char* file_path = "/alidata1/race2018/data/log";
    void *page;

    posix_memalign(&page, getpagesize(), getpagesize());
    cout << "page size:" << getpagesize() << endl;

    cout << "Open log-file" << endl;
    file_fd = open(file_path, O_RDWR | O_DIRECT);
    if (file_fd < -1) {
        cout << "Failed to open file!!" << endl;
    }

    gettimeofday(&start_tv, NULL);
    start_time = start_tv.tv_sec * 1000 + start_tv.tv_usec/1000;

    for (int i = 0;i < 1000000;i++) {
        read(file_fd, page, getpagesize());
    }

    gettimeofday(&stop_tv, NULL);
    stop_time = stop_tv.tv_sec * 1000 + stop_tv.tv_usec/1000;

    cout << "Done!need time:" << stop_time - start_time << "ms" << endl;

    return 1;
}

