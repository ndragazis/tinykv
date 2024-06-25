#include <sstream>
#include <iostream>

#include <seastar/util/log.hh>

#include "write-ahead-log.hh"

static seastar::logger lg(__FILE__);

WriteAheadLog::WriteAheadLog(const seastar::sstring& filename)
    : filename(filename) {
    lg.debug("Opening wal file: {} ", filename);
    ofs.open(filename, std::ios::out | std::ios::app);
    ifs.open(filename, std::ios::in);
}

WriteAheadLog::~WriteAheadLog() {
    lg.debug("Closing wal file: {} ", filename);
    if (ofs.is_open()) {
        ofs.close();
    }
    if (ifs.is_open()) {
        ifs.close();
    }
}

void WriteAheadLog::put(const seastar::sstring& key, const seastar::sstring& value) {
    ofs << "PUT" << ',' << key << ',' << value << '\n';
    ofs.flush();
}

void WriteAheadLog::remove(const seastar::sstring& key) {
    ofs << "REMOVE" << ',' << key << '\n';
    ofs.flush();
}

void WriteAheadLog::recover(std::function<void(const seastar::sstring&, const seastar::sstring&)> apply_put,
                            std::function<void(const seastar::sstring&)> apply_remove) {
    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::string op, key, value;
        std::getline(iss, op, ',');
        std::getline(iss, key, ',');
        if (op == "PUT") {
            std::getline(iss, value);
            apply_put(key, value);
        } else if (op == "REMOVE") {
            apply_remove(key);
        }
    }
}