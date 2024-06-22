#include <map>
#include <string>
#include <optional>
#include <format>
#include <iostream>

#include "memtable.hh"

const seastar::sstring MemTable::deletion_marker = "__deleted__";

MemTable::MemTable(const seastar::sstring& wal_filename)
    : _map(), curr_size(0), wal(wal_filename) {
    wal.recover(
    [this](const std::string& key, const std::string& value) {
        put(key, value, false);
    },
    [this](const std::string& key) {
        remove(key, false);
    });
}

int MemTable::size() const {
    return curr_size;
}

std::optional<seastar::sstring> MemTable::get(const seastar::sstring& key) const {
    std::cout << "MemTable - Searching for key " << key << "\n";
    auto find_it = _map.find(key);
    if (find_it != _map.end())
        return find_it->second;
    std::cout << "MemTable - Key not found.\n";
    return {};
}

void MemTable::put(const seastar::sstring key, seastar::sstring value, bool persist) {
    if (persist)
        wal.put(key, value);
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        _map[key] = value;
        return;
    }
    _map.emplace(key, value);
    increase_size(key, value);
    return;
}

void MemTable::remove(const seastar::sstring& key, bool persist) {
    if (persist)
        wal.remove(key);
    std::cout << "MemTable - Deleting key " << key << "\n";
    _map[key] = deletion_marker;
    return;
}

void MemTable::increase_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size += key.size() + value.size();
}

void MemTable::decrease_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size -= key.size() + value.size();
}