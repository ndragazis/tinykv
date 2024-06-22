#include <fstream>
#include <sstream>

#include "sstable.hh"

const seastar::sstring SSTable::deletion_marker = "__deleted__";

SSTable::SSTable(const seastar::sstring& filename)
    : filename(filename) {}

std::optional<seastar::sstring> SSTable::get(const seastar::sstring& key) const {
    std::ifstream ifs(filename);
    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::string curr_key, value;
        std::getline(iss, curr_key, ',');
        std::getline(iss, value);
        if (curr_key == key && value != deletion_marker) {
            return value;
        }
    }
    return std::nullopt;
}