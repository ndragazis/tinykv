#ifndef MEMTABLE_HH
#define MEMTABLE_HH
#include <map>
#include <string>
#include <optional>
#include <stdexcept>
#include <format>

#include <seastar/core/sstring.hh>

#include "write-ahead-log.hh"

class MemTable {
public:
    std::map<seastar::sstring, seastar::sstring> _map;
    WriteAheadLog wal;
private:
    int curr_size;
public:
    MemTable(const seastar::sstring& wal_filename);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring key, seastar::sstring value, bool persist=true);
    std::optional<seastar::sstring> remove(const seastar::sstring& key, bool persist=true);
    int size() const;
private:
    void increase_size(const seastar::sstring& key, const seastar::sstring& value);
    void decrease_size(const seastar::sstring& key, const seastar::sstring& value);
};

#endif // MEMTABLE_HH