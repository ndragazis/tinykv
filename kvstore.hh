#ifndef KVSTORE_HH
#define KVSTORE_HH
#include <list>

#include <seastar/core/future.hh>

#include "memtable.hh"

class KVStore {
private:
    const seastar::sstring dir;
    const seastar::sstring wal_filename;
    std::unique_ptr<MemTable> current_memtable;
    std::list<std::shared_ptr<MemTable>> active_memtables;
    const int flush_threshold;
    int wal_index;
public:
    KVStore(int memtable_size, const seastar::sstring& dir);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring& key, const seastar::sstring& value);
    std::optional<seastar::sstring> remove(const seastar::sstring& key);
private:
    seastar::future<> flush_memtable(std::shared_ptr<MemTable> memtable);
    void create_new_memtable();
};

#endif // KVSTORE_HH