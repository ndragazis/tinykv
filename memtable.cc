#include <map>
#include <string>
#include <optional>
#include <stdexcept>
#include <format>
#include <iostream>

#include "memtable.hh"

MemTable::MemTable(int max_size)
    : _map(), max_size(max_size) {
    }

std::optional<seastar::sstring> MemTable::get(const seastar::sstring& key) const {
    std::cout << "Searching for key " << key << "\n";
    auto find_it = _map.find(key);
    if (find_it != _map.end())
        return find_it->second;
    std::cout << "Key not found.\n";
    return {};
}

void MemTable::put(const seastar::sstring key, seastar::sstring value) {
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        _map[key] = value;
        return;
    }
    if (_map.size() < max_size) {
        _map.emplace(key, value);
        return;
    } else {
        //FIXME: Flush the memtable into an SSTable, and create a new empty memtable.
        throw std::runtime_error(std::format("Cannot insert key {} in memtable.", (std::string&)key));
    }
}

std::optional<seastar::sstring> MemTable::remove(const seastar::sstring& key) {
    std::cout << "Searching for key " << key << "\n";
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        auto value = find_it->second;
        _map.erase(key);
        return value;
    }
    std::cout << "Key not found.\n";
    return {};
}
