#include <sstream>
#include <iostream>
#include <algorithm>

#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/log.hh>
#include <seastar/util/file.hh>

#include "write-ahead-log.hh"

static seastar::logger lg(__FILE__);

WriteAheadLog::WriteAheadLog(const seastar::sstring& filename)
    : filename(filename) {
}

seastar::future<> WriteAheadLog::open() {
    lg.debug("Opening wal file: {} ", filename);
    auto flags = seastar::open_flags::wo | seastar::open_flags::create;
    f = co_await seastar::open_file_dma(filename, flags);
    fpos = 0;
    wbuf_pos = 0;
    wbuf_align = f.disk_write_dma_alignment();
    wbuf_len = std::min(static_cast<size_t>(512), f.disk_write_dma_alignment());
    lg.debug("Creating write buffer (alignment: {}, length: {})", wbuf_align, wbuf_len);
    wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
    memset(wbuf.get_write(), 0, wbuf_len);
}

seastar::future<> WriteAheadLog::close() {
    lg.debug("Closing wal file: {} ", filename);
    co_await f.close();
}

seastar::future<>
WriteAheadLog::put(const seastar::sstring& key, const seastar::sstring& value) {
    co_await write("PUT," + key + "," + value + "\n");
}

seastar::future<> WriteAheadLog::remove(const seastar::sstring& key) {
    co_await write("REMOVE," + key + "\n");
}

seastar::future<>
WriteAheadLog::write(const std::string& entry) {
    lg.debug("Adding new entry in WAL: {}", filename);
    lg.debug("Pre-state: wbuf_pos/size = {}/{}, fpos = {}",
            wbuf_pos, wbuf_len, fpos);
    size_t rbuf_len = entry.size();
    uint64_t rbuf_pos = 0;
    uint64_t transfer_chunk;
    while (rbuf_pos < entry.size()) {
        lg.debug("Writing chunk to file: "
                 "rbuf_pos/size = {}/{}, wbuf_pos/size = {}/{}, fpos = {}",
            rbuf_pos, rbuf_len, wbuf_pos, wbuf_len, fpos);
        if (wbuf_pos + (rbuf_len - rbuf_pos) < wbuf_len) {
            //Rest of rbuf fits inside the rest of wbuf. Copy all of it.
            transfer_chunk = rbuf_len - rbuf_pos;
        } else {
            //Rest of rbuf does not fit inside the rest of wbuf. Copy only what fits.
            transfer_chunk = wbuf_len - wbuf_pos;
        }
        memcpy(wbuf.get_write() + wbuf_pos, entry.c_str() + rbuf_pos, transfer_chunk);
        wbuf_pos += transfer_chunk;
        rbuf_pos += transfer_chunk;
        co_await f.dma_write(fpos, wbuf.get(), wbuf_len);
        co_await f.flush();
        if (wbuf_pos >= wbuf_len) {
            fpos += wbuf_len;
            memset(wbuf.get_write(), 0, wbuf_len);
            wbuf_pos = 0;
        }
    }
    lg.debug("Added new entry in WAL: {}", filename);
    lg.debug("Post-state: wbuf_pos/size = {}/{}, fpos = {}",
            wbuf_pos, wbuf_len, fpos);
}

seastar::future<> WriteAheadLog::recover(
    std::function<void(const seastar::sstring&, const seastar::sstring&)> apply_put,
    std::function<void(const seastar::sstring&)> apply_remove) {

    lg.debug("Recovering wal file: {}", filename);
    auto buf = co_await seastar::util::read_entire_file_contiguous(
        std::filesystem::path(filename));
    lg.debug("Read {} bytes.", buf.size());
    if (!buf.empty())
        lg.debug("Data read:\n{}", buf);
    std::istringstream iss(buf);
    std::string line;
    while (std::getline(iss, line)) {
        std::istringstream iss(line);
        std::string op, key, value;
        std::getline(iss, op, ',');
        lg.debug("Current opcode: {}", op);
        std::getline(iss, key, ',');
        lg.debug("Current key: {}", key);
        lg.debug("Current key characters (ASCII): '{:X}'", fmt::join(key, " "));
        if (op == "PUT") {
            std::getline(iss, value);
            lg.debug("Current value: {}", value);
            apply_put(key, value);
        } else if (op == "REMOVE") {
            apply_remove(key);
        }
    }
}