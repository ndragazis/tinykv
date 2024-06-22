#ifndef KVSTORE_HH
#define KVSTORE_HH
#include <list>

#include <seastar/core/future.hh>

#include "memtable.hh"
#include "sstable.hh"

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
public:
    KVStore(int memtable_size, const seastar::sstring& dir);
    ~KVStore();
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring& key, const seastar::sstring& value);
    void remove(const seastar::sstring& key);
private:
    seastar::future<> flush_memtable(std::shared_ptr<MemTable> memtable);
    void create_new_memtable();
    void recover_active_memtables();
    void load_sstables();
};

#endif // KVSTORE_HH