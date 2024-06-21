#include <map>
#include <string>
#include <optional>
#include <format>
#include <iostream>

#include "memtable.hh"

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
    std::cout << "Searching for key " << key << "\n";
    auto find_it = _map.find(key);
    if (find_it != _map.end())
        return find_it->second;
    std::cout << "Key not found.\n";
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

std::optional<seastar::sstring> MemTable::remove(const seastar::sstring& key, bool persist) {
    std::cout << "Searching for key " << key << "\n";
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        if (persist)
            wal.remove(key);
        auto value = find_it->second;
        _map.erase(key);
        decrease_size(key, value);
        return value;
    }
    std::cout << "Key not found.\n";
    return {};
}

void MemTable::increase_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size += key.size() + value.size();
}

void MemTable::decrease_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size -= key.size() + value.size();
}