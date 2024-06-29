#ifndef SSTABLE_H
#define SSTABLE_H
#include <optional>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/generator.hh>

class SSTable {
public:
    static const seastar::sstring deletion_marker;
    const seastar::sstring filename;
    SSTable(const seastar::sstring& filename);
    seastar::future<std::optional<seastar::sstring>>
    get(const seastar::sstring& key) const;
    seastar::coroutine::experimental::generator<std::pair<seastar::sstring, seastar::sstring>>
    get() const;
};

#endif // MEMTABLE_HH