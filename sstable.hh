#ifndef SSTABLE_H
#define SSTABLE_H
#include <optional>

#include <seastar/core/sstring.hh>

class SSTable {
public:
    static const seastar::sstring deletion_marker;
    seastar::sstring filename;
    SSTable(const seastar::sstring& filename);
    std::optional<seastar::sstring> get(const seastar::sstring& key) const;
};

#endif // MEMTABLE_HH