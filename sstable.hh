#ifndef SSTABLE_H
#define SSTABLE_H
#include <optional>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

class SSTable {
public:
    static const seastar::sstring deletion_marker;
    const seastar::sstring filename;
    SSTable(const seastar::sstring& filename);
    seastar::future<std::optional<seastar::sstring>>
    get(const seastar::sstring& key) const;
};

#endif // MEMTABLE_HH