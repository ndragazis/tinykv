#include <sstream>
#include <iostream>

#include "write-ahead-log.hh"

WriteAheadLog::WriteAheadLog(const seastar::sstring& filename)
    : filename(filename) {
    std::cout << "WriteAheadLog - opening wal file " << filename << "\n";
    ofs.open(filename, std::ios::out | std::ios::app);
    ifs.open(filename, std::ios::in);
}

WriteAheadLog::~WriteAheadLog() {
    std::cout << "WriteAheadLog - closing wal file " << filename << "\n";
    if (ofs.is_open()) {
        ofs.close();
    }
    if (ifs.is_open()) {
        ifs.close();
    }
}

void WriteAheadLog::put(const seastar::sstring& key, const seastar::sstring& value) {
    ofs << "PUT\t" << key << '\t' << value << '\n';
    ofs.flush();
}

void WriteAheadLog::remove(const seastar::sstring& key) {
    ofs << "REMOVE\t" << key << '\n';
    ofs.flush();
}

void WriteAheadLog::recover(std::function<void(const seastar::sstring&, const seastar::sstring&)> apply_put,
                            std::function<void(const seastar::sstring&)> apply_remove) {
    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::string op, key, value;
        std::getline(iss, op, '\t');
        std::getline(iss, key, '\t');
        if (op == "PUT") {
            std::getline(iss, value);
            apply_put(key, value);
        } else if (op == "REMOVE") {
            apply_remove(key);
        }
    }
}