## Tools and libraries on the target environment
1. CentOS 6.8
1. CMake 3.6.1
2. GCC 4.8.2
3. Boost 1.67
1. Libevent 2.0.22
1. Libz 1.2.3
1. Snappy 1.1.0
1. Lzma(xz-devel)   0.5.beta.20091007git.el6
1. lz4 r131
1. TBB [2018 Update 5](https://github.com/01org/tbb/releases)

## How to build

####1. Create a build directory

```commandline
mkdir build
cd build
```

####2. Build
##### 2.1 Build your shared library only
```commandline
cmake ..
make
```

##### 2.2 Build your shared library and dummy example
```commandline
cmake -DBUILD_EXAMPLE=ON ..
make
```

##### 2.2.1. Run sample executable program
```commandline
./sample
```



选手需要做的是重写queue_store.cpp,更多资料参考Java的Demo。


