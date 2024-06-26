#ifndef WRITEAHEADLOG_H
#define WRITEAHEADLOG_H
#include <fstream>

#include <seastar/core/sstring.hh>

class WriteAheadLog {
public:
    seastar::sstring filename;
private:
    std::ofstream ofs;
    std::ifstream ifs;
public:
    WriteAheadLog(const seastar::sstring& filename);
    void put(const seastar::sstring& key, const seastar::sstring& value);
    void remove(const seastar::sstring& key);
    void recover(std::function<void(const seastar::sstring&, const seastar::sstring&)> apply_put,
                 std::function<void(const seastar::sstring&)> apply_remove);
    seastar::future<> load();
    seastar::future<> destroy();
};

#endif // WRITEAHEADLOG_H