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
private:
    std::map<seastar::sstring, seastar::sstring> _map;
    int max_size;
    WriteAheadLog wal;
public:
    MemTable(int max_size, const seastar::sstring& wal_filename);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring key, seastar::sstring value);
    std::optional<seastar::sstring> remove(const seastar::sstring& key);
};

#endif // MEMTABLE_HH