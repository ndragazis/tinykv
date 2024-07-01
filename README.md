# tinykv

tinykv is a persistent key-value store based on the Seastar framework.

Seastar is an application framework for high-performance server-side applications.
* Website: http://seastar.io/
* Github: https://github.com/scylladb/seastar.git
* Documentation: http://docs.seastar.io/master/index.html
* Tutorial: http://docs.seastar.io/master/tutorial.html

## Prerequisites

`docker`

## Setup

```
$ docker build -t tinykv .
$ docker run --rm -it -v $(pwd):/home/src tinykv
```
The rest of the instructions assume this setup is completed and that you are in
the running docker container.

## Building the app

```
$ make app
```

Note that the first invocation of this command might take a while as it will
also compile seastar. Subsequent builds will be much faster.

## Running the app

```
$ ./app
```

Don't be alarmed by warnings like this:
```
WARNING: unable to mbind shard memory; performance may suffer: Operation not permitted
WARN  2024-06-25 19:07:18,615 seastar - Requested AIO slots too large, please increase request capacity in /proc/sys/fs/aio-max-nr. configured:65536 available:65536 requested:88208
WARN  2024-06-25 19:07:18,615 seastar - max-networking-io-control-blocks adjusted from 10000 to 7166, since AIO slots are unavailable
INFO  2024-06-25 19:07:18,615 seastar - Reactor backend: io_uring
WARN  2024-06-25 19:07:18,617 seastar - Creation of perf_event based stall detector failed: falling back to posix timer: std::system_error (error system:1, perf_event_open() failed: Operation not permitted)
```

## Usage

```
# put "potter" in key "harry" (value is updated if key already exists)
curl -v -XPUT http://127.0.0.1:9999/keys/harry -d potter

# get key "harry" (should be "potter")
curl -v -XGET http://127.0.0.1:9999/keys/harry

# delete key "harry"
curl -v -XDELETE http://127.0.0.1:9999/keys/harry

# get sorted list of all keys
curl -v -XGET http://127.0.0.1:9999/keys
```

To see the available seastar options:
```
$ ./app --help-seastar
```
