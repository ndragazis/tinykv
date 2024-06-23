#include <iostream>
#include <regex>

#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include "file-utils.hh"

/*
 * Extract numeric identifier from filename
 */
int extractNumber(const std::string& filename, const std::string& prefix) {
    std::regex re(prefix + "(\\d+)");
    std::smatch match;
    if (std::regex_search(filename, match, re) && match.size() > 1) {
        return std::stoi(match.str(1));
    }
    return -1;
}

/*
 * Find files with a specific prefix
 *
 * Code inspired from:
 * https://github.com/scylladb/scylladb/blob/a7e38ada8e313bebef11448c9cbb22d0956942bf/db/commitlog/commitlog.cc#L1577
 */
seastar::future<std::vector<std::string>>
find_files(const std::string& directory, const std::string& prefix) {
    std::vector<std::string> matchingFiles;
    seastar::file f = co_await seastar::open_directory(directory);

    auto h = f.list_directory([&](seastar::directory_entry de) -> seastar::future<> {
        if (de.type == seastar::directory_entry_type::regular
            && de.name.find(prefix) == 0) {
            matchingFiles.emplace_back(directory + "/" + static_cast<std::string>(de.name));
        }
        return seastar::make_ready_future<>();
    });
    co_await h.done();
    co_return matchingFiles;
}