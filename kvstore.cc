#include "kvstore.hh"

KVStore::KVStore(int memtable_size)
    : memtable(memtable_size) {}

std::optional<seastar::sstring> KVStore::get(const seastar::sstring& key) const {
    return memtable.get(key);
}

void KVStore::put(const seastar::sstring& key, const seastar::sstring& value) {
    memtable.put(key, value);
}

std::optional<seastar::sstring> KVStore::remove(const seastar::sstring& key) {
    return memtable.remove(key);
}
