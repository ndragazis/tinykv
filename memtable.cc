#include <map>
#include <string>
#include <optional>
#include <format>
#include <iostream>

#include <seastar/util/log.hh>

#include "memtable.hh"

static seastar::logger lg(__FILE__);

const seastar::sstring MemTable::deletion_marker = "__deleted__";

MemTable::MemTable(const seastar::sstring& wal_filename)
    : _map(), curr_size(0), wal(wal_filename) {
}

seastar::future<> MemTable::load() {
    co_await wal.open();
    co_await wal.recover(
    [this](const std::string& key, const std::string& value) {
        _put(key, value);
    },
    [this](const std::string& key) {
        _put(key, deletion_marker);
    });
}

seastar::future<> MemTable::destroy() {
    co_await wal.close();
}

int MemTable::size() const {
    return curr_size;
}

std::optional<seastar::sstring> MemTable::get(const seastar::sstring& key) const {
    lg.debug("Getting key {} from memtable (wal: {})", key, wal.filename);
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        return find_it->second;
        lg.debug("Found the key!");
    }
    lg.debug("Did not find key {} in memtable (wal: {})", key, wal.filename);
    return std::nullopt;
}

void MemTable::_put(const seastar::sstring key, seastar::sstring value) {
    auto find_it = _map.find(key);
    if (find_it != _map.end()) {
        _map[key] = value;
        return;
    }
    _map.emplace(key, value);
    increase_size(key, value);
    return;
}

seastar::future<>
MemTable::put(const seastar::sstring key, seastar::sstring value) {
    lg.debug("Putting key {} in memtable (wal: {})", key, wal.filename);
    co_await wal.put(key, value);
    _put(key, value);
    co_return;
}

seastar::future<> MemTable::remove(const seastar::sstring& key) {
    lg.debug("Deleting key {} from memtable (wal: {})", key, wal.filename);
    co_await wal.remove(key);
    _put(key, deletion_marker);
    co_return;
}

void MemTable::increase_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size += key.size() + value.size();
}

void MemTable::decrease_size(const seastar::sstring& key, const seastar::sstring& value) {
    curr_size -= key.size() + value.size();
}

auto MemTable::begin() -> decltype(_map.begin()) {
    return _map.begin();
}

auto MemTable::end() -> decltype(_map.end()) {
    return _map.end();
}