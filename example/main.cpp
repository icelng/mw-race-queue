//
// Created by yiran on 18-7-5.
//

#include <iostream>
#include <malloc.h>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <random>
#include <ctime>
//#include "gperftools/profiler.h"

#include "queue_store.h"

using namespace race2018;

#define MSG "01234567890123456789012345678901234567890123456789"

class RandNum_generator
{
private:
    RandNum_generator(RandNum_generator const&)=delete;
    RandNum_generator& operator=(RandNum_generator const&)=delete;
    std::uniform_int_distribution<int> u;
    std::default_random_engine e;
    int m_start, m_end;
public:
    // [start, end], inclusive, uniformally distributed
    RandNum_generator(int start, int end)
            : u(start, end), e(time(nullptr)),
              m_start(start), m_end(end)
    {}

    // [m_start, m_end], inclusive
    int nextInt()
    {
        return u(e);
    }

    // [0, max], inclusive
    int nextInt(int max)
    {
        return int((u(e) - m_start) / float(m_end - m_start) * max);
    }
};

void produce(queue_store& queueStore, int number,
             std::chrono::time_point<std::chrono::high_resolution_clock> const& maxTimeStamp, int maxMsgNum,
             std::atomic_long& counter, std::unordered_map<std::string, std::atomic_int>& queueCounter,
             std::unordered_map<std::string, std::mutex>& queueLock)
{
    long count;
    while ((count = counter.fetch_add(1)) < maxMsgNum
           && std::chrono::high_resolution_clock::now() <= maxTimeStamp) {
        if (count % 1000000 == 0) {
            std::cout << "send cnt:" << count << std::endl;
        }
        std::string queueName("Queue-" + std::to_string(count % queueCounter.size()));
        {
            std::lock_guard<std::mutex> lock(queueLock[queueName]);
            MemBlock memBlock;
//            memBlock.size = strlen(MSG) + 1;
//            memBlock.ptr = malloc(memBlock.size);
//            strcpy(static_cast<char *>(memBlock.ptr), MSG);
//            queueCounter[queueName].fetch_add(1);
            std::string msg_string = std::to_string(queueCounter[queueName].fetch_add(1));
            const char* msg = msg_string.c_str();
            size_t msg_size = strlen(msg);
            memBlock.size = msg_size * 20 + 1;
            memBlock.ptr = malloc(memBlock.size);
            for (int k = 0;k < 20;k++) {
                memcpy(memBlock.ptr + (msg_size * k), msg, msg_size);
//                strcpy(static_cast<char *>(memBlock.ptr + ), msg);
            }
            queueStore.put(queueName, memBlock);
        }
    }
}

void checkIndex(queue_store& queueStore, int number,
                std::chrono::time_point<std::chrono::high_resolution_clock> const& maxTimeStamp, int maxMsgNum,
                std::atomic_long& counter, std::unordered_map<std::string, std::atomic_int>& queueCounter)
{
    RandNum_generator rng(0, 10000);
    while (counter.fetch_add(1) < maxMsgNum
           && std::chrono::high_resolution_clock::now() <= maxTimeStamp) {
        std::string queueName("Queue-" + std::to_string(rng.nextInt(queueCounter.size() - 1)));
        int index = rng.nextInt(queueCounter[queueName] - 1) - 10;
        if (index < 0) index = 0;
        std::vector<MemBlock> msgs = queueStore.get(queueName, index, 10);
        for (auto const& msg : msgs) {
            int msg_int = index;
            int msg_len = 1;
            msg_int = msg_int / 10;
            while (msg_int != 0) {
                msg_len++;
                msg_int = msg_int / 10;
            }
            ((char *)msg.ptr)[msg_len] = 0;
//            index++;
//            if (strcmp(MSG, static_cast<const char *>(msg.ptr)) != 0) {
            if (strcmp(std::to_string(index++).c_str(), static_cast<const char *>(msg.ptr)) != 0) {
                std::cout << "Check error:" << (char *)msg.ptr << ", need:"<< index - 1 <<  std::endl;
                exit(-1);
            }
            free(msg.ptr);
        }
//        std::cout << "check index accept!" << counter << std::endl;
    }
}

void consume(queue_store& queueStore, int number,
             std::chrono::time_point<std::chrono::high_resolution_clock> const& maxTimeStamp,
             std::atomic_long& counter, std::unordered_map<std::string, int>& offsets)
{
    std::cout << "consumer started" << std::endl;
    std::unordered_map<std::string, std::atomic_int> pullOffsets;
    std::atomic_long check_num;
    check_num = 0;
    for (auto const& p : offsets) {
        pullOffsets[p.first];
    }

    // May not work in C++ if deleting items while iterating through the container;
    // The following is a safer shot.
    /*
    while (pullOffsets.size() > 0 && std::chrono::high_resolution_clock::now() <= maxTimeStamp) {
        for (auto const& p : pullOffsets) {
            auto& queueName = p.first;
            int index = p.second;
            auto msgs = queueStore.get(queueName, index, 10);
            if (msgs.size() > 0) {
                pullOffsets[queueName].fetch_add(msgs.size());
                for (auto const& msg : msgs) {
                    if (msg != std::to_string(index++)) {
                        std::cout << queueName << " " << msg << " " << (index - 1) << std::endl;
                        std::cout << "Check error" << std::endl;
                        exit(-1);
                    }
                }
                counter.fetch_add(msgs.size());
            }
            if (msgs.size() < 10) {
                if (pullOffsets[queueName] != offsets[queueName]) {
                    std::cout << queueName << " " << pullOffsets[queueName] << " " << offsets[queueName] << std::endl;
                    std::cout << "Queue Number Error" << std::endl;
                    exit(-1);
                }
                pullOffsets.erase(queueName);
            }
        }
    }
    */

    for (auto const& p : pullOffsets) {
        auto& queueName = p.first;
        while (std::chrono::high_resolution_clock::now() <= maxTimeStamp) {
            int index = p.second;
//            std::cout << "---------getting msg" << std::endl;
            std::vector<MemBlock> msgs = queueStore.get(queueName, index, 10);
            if (!msgs.empty()) {
                pullOffsets[queueName].fetch_add(static_cast<int>(msgs.size()));
                for (auto const& msg : msgs) {
                    int msg_int = index;
                    int msg_len = 1;
                    msg_int = msg_int / 10;
                    while (msg_int != 0) {
                        msg_len++;
                        msg_int = msg_int / 10;
                    }
                    ((char *)msg.ptr)[msg_len] = 0;
//                    index++;
//                    if (strcmp(MSG, static_cast<const char *>(msg.ptr)) != 0) {
                    if (strcmp(std::to_string(index++).c_str(), static_cast<const char *>(msg.ptr)) != 0) {
                        std::cout << "Check error" << std::endl;
                        exit(-1);
                    }
                    free(msg.ptr);
                }
                counter.fetch_add(msgs.size());
            }
            if (msgs.size() < 10) {
                if (pullOffsets[queueName] != offsets[queueName]) {
                    std::cout << "Queue Number Error, pullOffset:" << pullOffsets[queueName] << ", offset:" << offsets[queueName] << std::endl;
                    exit(-1);
                }
                break;
            }
//            std::cout << "consumer accept!" << counter << std::endl;
        }
    }
}

int main(int argc, char* argv[])
{
    //评测相关配置
    //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
//    int msgNum  = 200000000;
//    int msgNum  = 200000000;
    int msgNum  = 200000;
    //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
    int sendTime = 30 * 60 * 1000;
    //消费阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
    int checkTime = 30 * 60 * 1000;
    //队列的数量
//    int queueNum = 1000000;
    int queueNum = 10000;
    //正确性检测的次数
    int checkNum = static_cast<int>(queueNum * 1.5);
    //消费阶段的总队列数量
    int checkQueueNum = 20000;
    //发送的线程数量
    int sendTsNum = 10;
    //消费的线程数量
    int checkTsNum = 10;

    std::cout << "msgNum:" << msgNum << " queueNum:" << queueNum << " checkNum:" << checkNum << " checkQueueNum:" << checkQueueNum << std::endl;

    std::unordered_map<std::string, std::atomic_int> queueNumMap;
    std::unordered_map<std::string, std::mutex> queueLockMap;
    for (int i = 0; i < queueNum; ++i) {
        std::string queueName{"Queue-" + std::to_string(i)};
        queueNumMap[queueName];
        queueLockMap[queueName];
    }

//    ProfilerStart("profile");

    queue_store queueStore;

    //Step1: 发送消息
    auto sendStart = std::chrono::high_resolution_clock::now();
    auto maxTimeStamp = sendStart + std::chrono::duration<long, std::milli>(sendTime);
    std::atomic_long sendCounter{0};
    std::vector<std::unique_ptr<std::thread>> sends;
    for (int i = 0; i < sendTsNum; ++i) {
        std::unique_ptr<std::thread> th(new std::thread(produce, std::ref(queueStore), i, std::ref(maxTimeStamp),
                                                        msgNum, std::ref(sendCounter), std::ref(queueNumMap),
                                                        std::ref(queueLockMap)));
        sends.push_back(std::move(th));
    }
    for (auto& th : sends) {
        th->join();
    }
    auto sendSend = std::chrono::high_resolution_clock::now();
    std::cout << "Send: " << std::chrono::duration<double, std::milli>(sendSend - sendStart).count()
              << " ms Num: " << sendCounter << std::endl;

//    ProfilerStop();

    //Step2: 索引的正确性校验
    auto indexCheckStart = std::chrono::high_resolution_clock::now();
    auto maxCheckTime = indexCheckStart + std::chrono::duration<long, std::milli>(checkTime);
    std::atomic_long indexCheckCounter{0};
    std::vector<std::unique_ptr<std::thread>> indexChecks;
    for (int i = 0; i < sendTsNum; ++i) {
        std::unique_ptr<std::thread> th(new std::thread(checkIndex, std::ref(queueStore), i, std::ref(maxCheckTime),
                                                        checkNum, std::ref(indexCheckCounter), std::ref(queueNumMap)));
        indexChecks.push_back(std::move(th));
    }

    for (auto& th : indexChecks) {
        th->join();
    }
    auto indexCheckEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Index Check: " << std::chrono::duration<double, std::milli>(indexCheckEnd - indexCheckStart).count()
              << " ms Num: " << indexCheckCounter << std::endl;

    //Step3: 消费消息，并验证顺序性
    auto checkStart = std::chrono::high_resolution_clock::now();
    RandNum_generator rng(0, 10000);
    std::atomic_long checkCounter{0};
    std::vector<std::unordered_map<std::string, int>> offsetses;
    std::vector<std::unique_ptr<std::thread>> checks;
    std::cout << "Preparing start consumer" << std::endl;
    for (int i = 0; i < sendTsNum; ++i) {
        int eachCheckQueueNum = checkQueueNum / checkTsNum;
        std::unordered_map<std::string, int> offsets;
        for (int j = 0; j < eachCheckQueueNum; ++j) {
            std::string queueName = "Queue-" + std::to_string(rng.nextInt(queueNum - 1));
            while (offsets.find(queueName) != offsets.end()) {
                queueName = "Queue-" + std::to_string(rng.nextInt(queueNum - 1));
            }
            offsets[queueName] = queueNumMap[queueName];
        }
        offsetses.push_back(offsets);
    }
    std::cout << "Starting done!" << std::endl;
    for (int i = 0; i < sendTsNum; ++i) {
        std::unique_ptr<std::thread> th(new std::thread(consume, std::ref(queueStore), i, std::ref(maxCheckTime),
                                                        std::ref(checkCounter), std::ref(offsetses[i])));
        checks.push_back(std::move(th));
    }
    std::cout << "Consumer started done!" << std::endl;
    for (auto& th : checks) {
        th->join();
    }
    auto checkEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Check: " << std::chrono::duration<double, std::milli>(checkEnd - checkStart).count()
              << " ms Num: " << checkCounter << std::endl;

    //评测结果
    auto denom = std::chrono::duration<double, std::milli>(sendSend - sendStart).count() +
                 std::chrono::duration<double, std::milli>(checkEnd - checkStart).count() +
                 std::chrono::duration<double, std::milli>(indexCheckEnd - indexCheckStart).count();
    std::cout << "Tps: "
              << (sendCounter + checkCounter + indexCheckCounter + 0.1) * 1000 / denom
              << std::endl;

    return 0;
}