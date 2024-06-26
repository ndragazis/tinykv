#ifndef MEMTABLE_HH
#define MEMTABLE_HH
#include <map>
#include <string>
#include <optional>
#include <stdexcept>
#include <format>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>

#include "write-ahead-log.hh"

class MemTable {
public:
    static const seastar::sstring deletion_marker;
    WriteAheadLog wal;
private:
    std::map<seastar::sstring, std::optional<seastar::sstring>> _map;
    int curr_size;
public:
    MemTable(const seastar::sstring& wal_filename);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    seastar::future<> put(const seastar::sstring key, seastar::sstring value);
    seastar::future<> remove(const seastar::sstring& key);
    seastar::future<> load();
    seastar::future<> destroy();
    int size() const;
    auto begin() -> decltype(_map.begin());
    auto end() -> decltype(_map.end());
private:
    void increase_size(const seastar::sstring& key, const seastar::sstring& value);
    void decrease_size(const seastar::sstring& key, const seastar::sstring& value);
    void _put(const seastar::sstring key, seastar::sstring value);
};

#endif // MEMTABLE_HH