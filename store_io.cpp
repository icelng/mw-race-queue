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

void* flush_service(void* arg) {
    StoreIO *store_io = (StoreIO*) arg;

    store_io->do_flush();

}

StoreIO::StoreIO(const char* file_path,
                 u_int64_t file_size,
                 u_int64_t region_size,
                 size_t buffers_num,
                 size_t buffer_size) {

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
    file_fd = open(file_path, O_RDWR | O_CREAT | O_DIRECT, 0644);
    ftruncate(file_fd, file_size);
    regions = static_cast<void **>(malloc(sizeof(void*) * regions_num));
    for (int i = 0;i < regions_num;i++) {
        regions[i] = mmap(NULL, this->region_size, PROT_READ, MAP_SHARED, file_fd, (long) i * this->region_size);
        if (regions[i] == reinterpret_cast<void *>(-1)) {
            cout << "Failed to map file!!!" << strerror(errno) << endl;
        }
        madvise(regions[i], this->region_size, MADV_RANDOM);  // 设置为随机访问
        printf("mapped region:0x%lx, phy_address:0x%lx, region_size:%dM\n", regions[i], (long) i * this->region_size, ((this->region_size) >> 20));
        cout << "mapped region:" << regions[i] << ", region_size:" << ((this->region_size) >> 20) << "M" << endl;
    }
    printf("Mapped region_mask:0x%x\n", region_mask);

    cout << "Mapped file successfully!" << endl;


    /*关于写缓存配置*/
    this->buffer_size = buffer_size;
    sem_init(&this->buffers_num, 0, buffers_num);
    sem_init(&this->flush_req_num, 0, 0);
    sem_init(&this->is_flushing, 0, 1);
    max_flush_queue_len = buffers_num;
    flush_queue = (FlushRequestNode*) malloc(sizeof(FlushRequestNode) * (buffers_num + 1));
    pthread_spin_init(&flush_queue_lock, 0);
    flush_req_num_atomic = 0;
    buffer_now = NULL;
    flush_q_head = 0;
    flush_q_tail = 0;

    for (int i = 0;i < buffers_num;i++) {
        void* buffer;
        posix_memalign(&buffer, getpagesize(), buffer_size);
        buffers.push(buffer);
    }

    pthread_t tid;
    pthread_create(&tid, NULL, flush_service, this);

}

void* StoreIO::get_region(u_int64_t addr) {
    u_int32_t region_no = static_cast<u_int32_t>(addr >> region_bits_len);

    if (region_no > regions_num) {
        cout << "Illegal address when get_region!" << endl;
    }

    return regions[region_no];
}

void StoreIO::flush() {
    std::lock_guard<std::mutex> lock(flush_mutex);
    if (buffer_now != NULL) {
        pthread_spin_lock(&flush_queue_lock);
        flush_queue[flush_q_tail % (max_flush_queue_len + 1)].buffer = buffer_now;
        flush_queue[flush_q_tail++ % (max_flush_queue_len + 1)].flush_size = buffer_offset;
        pthread_spin_unlock(&flush_queue_lock);
        if (flush_req_num_atomic.fetch_add(1) == 0) {
            sem_wait(&is_flushing);
        }
        sem_post(&flush_req_num);
        buffer_now = NULL;
    }

}

/**
 * 最好是对齐页的
 * */
void StoreIO::write_data(void *data, size_t data_size) {
    std::lock_guard<std::mutex> lock(flush_mutex);
    u_int64_t data_offset = 0;

    if (buffer_now == NULL) {
        sem_wait(&buffers_num);
        buffers.try_pop(buffer_now);
        buffer_offset = 0;
    }

    while (data_offset < data_size) {
        size_t this_write_size = std::min(buffer_size - buffer_offset, data_size - data_offset);

        memcpy(buffer_now + buffer_offset, data + data_offset, this_write_size);
        buffer_offset += this_write_size;
        data_offset += this_write_size;

        if (buffer_offset == buffer_size) {
            /*提交flush*/
            pthread_spin_lock(&flush_queue_lock);
            flush_queue[flush_q_tail % (max_flush_queue_len + 1)].buffer = buffer_now;
            flush_queue[flush_q_tail++ % (max_flush_queue_len + 1)].flush_size = buffer_offset;
            pthread_spin_unlock(&flush_queue_lock);
            if (flush_req_num_atomic.fetch_add(1) == 0) {
                sem_wait(&is_flushing);
            }
            sem_post(&flush_req_num);

            if (data_offset < data_size) {
                /*还需要申请buffer来写*/
                sem_wait(&buffers_num);
                buffers.try_pop(buffer_now);
                buffer_offset = 0;
            } else {
                /*否则, 退出*/
                buffer_now = NULL;
                buffer_offset = 0;
                break;
            }
        }

    }

}

void StoreIO::do_flush() {
    FlushRequestNode req_node;

    while (true) {
        sem_wait(&flush_req_num);

        pthread_spin_lock(&flush_queue_lock);
        req_node.buffer = flush_queue[flush_q_head % (max_flush_queue_len + 1)].buffer;
        req_node.flush_size = flush_queue[flush_q_head++ % (max_flush_queue_len + 1)].flush_size;
        pthread_spin_unlock(&flush_queue_lock);

        /*写*/
        if (write(file_fd, req_node.buffer, req_node.flush_size) != req_node.flush_size) {
            cout << "Failed to write data to file!!!" << strerror(errno) << endl;
        }

        buffers.push(req_node.buffer);
        sem_post(&buffers_num);
        if (flush_req_num_atomic.fetch_sub(1) == 1) {
            sem_post(&is_flushing);
        }
    }

}

void StoreIO::wait_flush_done() {
    sem_wait(&is_flushing);
    sem_post(&is_flushing);
}

void StoreIO::add_offset(u_int64_t offset) {
    buffer_offset += offset;
}


