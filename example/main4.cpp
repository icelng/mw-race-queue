#include <iostream>
#include <cstring>
#include "queue_store.h"

using namespace std;
using namespace race2018;



int main4(int argc, char* argv[]) {

    queue_store store;

    for (int i = 0; i < 16; i++) {
        string slogan = string("abc") + to_string(i);
        char* data = new char[slogan.size() + 1];
        strcpy(data, slogan.c_str());
        MemBlock msg = {static_cast<void*>(data),
                        static_cast<size_t>(slogan.size())
        };

        store.put("Queue-1", msg);
    }

    vector<MemBlock> list = store.get("Queue-1", 10, 10);

    for (MemBlock &item : list) {
        char* msg = static_cast<char *>(item.ptr);
        cout << msg << endl;
    }

    return 0;
}