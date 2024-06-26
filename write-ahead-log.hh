#ifndef WRITEAHEADLOG_H
#define WRITEAHEADLOG_H
#include <fstream>

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

class WriteAheadLog {
public:
    seastar::sstring filename;
private:
    seastar::file f;
    uint64_t fpos;
    seastar::temporary_buffer<char> wbuf;
    uint64_t wbuf_pos;
    uint64_t wbuf_align;
    size_t wbuf_len;
public:
    WriteAheadLog(const seastar::sstring& filename);
    seastar::future<> put(const seastar::sstring& key, const seastar::sstring& value);
    seastar::future<> remove(const seastar::sstring& key);
    seastar::future<> recover(
        std::function<void(const seastar::sstring&, const seastar::sstring&)> apply_put,
        std::function<void(const seastar::sstring&)> apply_remove);
    seastar::future<> open();
    seastar::future<> close();
private:
    seastar::future<> write(const std::string& entry);
};

#endif // WRITEAHEADLOG_H