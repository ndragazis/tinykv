#ifndef KVSTORE_HH
#define KVSTORE_HH
#include <list>

#include <seastar/core/future.hh>
#include <seastar/coroutine/generator.hh>

#include "memtable.hh"
#include "sstable.hh"
#include "cache.hh"

class KVStore {
private:
    const seastar::sstring dir;
    const seastar::sstring wal_filename;
    std::unique_ptr<MemTable> current_memtable;
    //TODO: Not sure if the list should hold shared pointers or unique pointers.
    //Will revisit later.
    std::list<std::shared_ptr<MemTable>> active_memtables;
    std::list<SSTable> sstables;
    const int flush_threshold;
    int wal_index;
    int sstable_index;
    Cache cache;
public:
    KVStore(int memtable_size, int cache_capacity, const seastar::sstring& dir);
    seastar::future<std::optional<seastar::sstring>> get(const seastar::sstring& key);
    seastar::coroutine::experimental::generator<std::pair<seastar::sstring, seastar::sstring>>
    get() const;
    seastar::future<> put(const seastar::sstring& key, const seastar::sstring& value);
    seastar::future<> remove(const seastar::sstring& key);
    seastar::future<> start();
    seastar::future<> stop() noexcept;
private:
    seastar::future<> flush_memtable(std::shared_ptr<MemTable> memtable);
    seastar::future<> delete_memtable(std::shared_ptr<MemTable> memtable);
    seastar::future<> create_new_memtable();
    seastar::future<> recover_active_memtables();
    seastar::future<> load_sstables();
};

#endif // KVSTORE_HH