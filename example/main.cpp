#include <iostream>
#include <cstring>
#include <boost/filesystem.hpp>
#include "queue_store.h"

using namespace std;
using namespace race2018;



int main(int argc, char* argv[]) {

    queue_store store;

    for (int i = 0; i < 16; i++) {
        string slogan = string("abc") + to_string(i);
        char* data = new char[slogan.size() + 1];
        strcpy(data, slogan.c_str());
        MemBlock msg(data, strlen(data));
        store.put("Queue-1", msg);
    }

    vector<MemBlock> list = store.get("Queue-1", 10, 10);

    for (MemBlock &item : list) {
        cout << item.to_string() << endl;
    }

    return 0;
}