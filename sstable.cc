#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>
#include <seastar/util/file.hh>

#include <boost/algorithm/string.hpp>

#include "sstable.hh"

static seastar::logger lg(__FILE__);

const seastar::sstring SSTable::deletion_marker = "__deleted__";

SSTable::SSTable(const seastar::sstring& filename)
    : filename(filename) {}

/*
 * FIXME: Do not load the whole file in memory. Use pagination.
 */
seastar::coroutine::experimental::generator<std::pair<seastar::sstring, seastar::sstring>>
SSTable::get() const {
    auto buf = co_await seastar::util::read_entire_file_contiguous(std::filesystem::path(filename));
    lg.debug("Read {} bytes.", buf.size());
    if (!buf.empty())
        lg.debug("Data read:\n{}", buf);
    std::istringstream iss(buf);
    std::string line;
    while (std::getline(iss, line)) {
        std::istringstream iss(line);
        std::string key, value;
        std::getline(iss, key, ',');
        lg.debug("Current key: {}", key);
        lg.debug("Current key characters (ASCII): '{:X}'", fmt::join(key, " "));
        std::getline(iss, value);
        lg.debug("Current value: {}", value);
        co_yield std::pair(key, value);
    }
}

seastar::future<std::optional<seastar::sstring>>
SSTable::get(const seastar::sstring& key) const {
    lg.debug("Key in search: {}", key);
    lg.debug("Key characters (ASCII): '{:X}'", fmt::join(key, " "));
    auto kvs = get();
    while (auto kv = co_await kvs()) {
        auto [curr_key, value] = kv.value();
        if (curr_key == key) {
            if (value != deletion_marker) {
                lg.debug("Found the key!");
            } else {
                lg.debug("Key has been deleted.");
            }
            co_return value;
        }
    }
    lg.debug("Did not find key {} in sstable {}.", key, filename);
    co_return std::nullopt;
}