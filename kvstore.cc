#include <filesystem>
#include <iostream>

#include "kvstore.hh"

//KVStore::KVStore(int memtable_size, const seastar::sstring& dir)
//    : dir([&dir]() {
//        std::cout << "KVStore - Creating directory " << dir << "\n";
//        std::filesystem::create_directories(std::filesystem::path(dir));
//        return dir;
//    }())
//    , wal_filename(dir + "/wal")
//    , memtable(memtable_size, wal_filename) {
//}

KVStore::KVStore(int memtable_size, const seastar::sstring& dir)
    : dir(dir)
    , wal_filename(dir + "/wal")
{
    std::cout << "KVStore - Creating directory " << dir << "\n";
    std::filesystem::create_directories(std::filesystem::path(dir));
    memtable = std::make_unique<MemTable>(memtable_size, wal_filename);
}

std::optional<seastar::sstring> KVStore::get(const seastar::sstring& key) const {
    return memtable->get(key);
}

void KVStore::put(const seastar::sstring& key, const seastar::sstring& value) {
    memtable->put(key, value);
}

std::optional<seastar::sstring> KVStore::remove(const seastar::sstring& key) {
    return memtable->remove(key);
}
