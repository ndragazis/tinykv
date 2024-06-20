#ifndef KVSTORE_HH
#define KVSTORE_HH

#include "memtable.hh"

class KVStore {
private:
    MemTable memtable;
public:
    KVStore(int memtable_size);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring& key, const seastar::sstring& value);
    std::optional<seastar::sstring> remove(const seastar::sstring& key);
};

#endif // KVSTORE_HH