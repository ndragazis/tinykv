#ifndef MEMTABLE_HH
#define MEMTABLE_HH
#include <map>
#include <string>
#include <optional>
#include <stdexcept>
#include <format>

#include <seastar/core/sstring.hh>

class MemTable {
private:
    std::map<seastar::sstring, seastar::sstring> _map;
    int max_size;
public:
    MemTable(int max_size);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
    void put(const seastar::sstring key, seastar::sstring value);
    void remove(seastar::sstring key);
};

#endif // MEMTABLE_HH