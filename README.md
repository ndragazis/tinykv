# Ready-to-use stub seastar application

Seastar is an application framework for high-performance server-side applications.
* Website: http://seastar.io/
* Github: https://github.com/scylladb/seastar.git
* Documentation: http://docs.seastar.io/master/index.html
* Tutorial: http://docs.seastar.io/master/tutorial.html

Building seastar can be quite involved. This repo attempts to fast-track your way to your first seastar app, by providing a simple hello-world stub application, with simple build instruction that only require docker to work. Note that the setup instructions assume a UNIX-like environment (Linux, MacOS or WSL) but should work on Windows with minimal tweaking.

## Prerequisites

`docker`

## Setup

```
$ docker build -t seastar-app-stub .
$ docker run --rm -it -v $(pwd):/home/src seastar-app-stub
```
The rest of the instructions assume this setup is completed and that you are in the running docker container.

## Building the app

```
$ make app
```

Note that the first invocation of this command might take a while as it will also compile seastar. Subsequent builds will be much faster.

## Running the app

```
$ ./app
```

Don't be alarmed by warnings like this:
```
WARN  2022-05-12 12:49:21,493 [shard 0] seastar - Creation of perf_event based stall detector failed, falling back to posix timer: std::system_error (error system:1, perf_event_open() failed: Operation not permitted)
WARN  2022-05-12 12:49:21,493 [shard 0] seastar - Unable to set SCHED_FIFO scheduling policy for timer thread; latency impact possible. Try adding CAP_SYS_NICE
INFO  2022-05-12 12:49:21,493 [shard 0] seastar - Created fair group io-queue-0, capacity rate 2147483:2147483, limit 12582912, rate 16777216 (factor 1), threshold 2000
INFO  2022-05-12 12:49:21,493 [shard 0] seastar - Created io group dev(0), length limit 4194304:4194304, rate 2147483647:2147483647
INFO  2022-05-12 12:49:21,493 [shard 0] seastar - Created io queue dev(0) capacities: 512:2000/2000 1024:3000/3000 2048:5000/5000 4096:9000/9000 8192:17000/17000 16384:33000/33000 32768:65000/65000 65536:129000/129000 131072:257000/257000
WARN  2022-05-12 12:49:21,598 [shard 1] seastar - Creation of perf_event based stall detector failed, falling back to posix timer: std::system_error (error system:1, perf_event_open() failed: Operation not permitted)
```
If you see the `HELLO WORLD` printed at the bottom, the app is working:
```
INFO  2022-05-12 12:50:42,330 [shard 0] app.cc - HELLO WORLD
```

To see the available seastar options:
```
$ ./app --help-seastar
```
