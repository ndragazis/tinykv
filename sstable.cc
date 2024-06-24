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

//First attempt
//seastar::future<std::optional<seastar::sstring>>
//SSTable::get(const seastar::sstring& key) const {
//    return seastar::with_file(
//        seastar::open_file_dma(filename, seastar::open_flags::ro),
//        [this, &key](seastar::file& f) -> seastar::future<std::optional<seastar::sstring>> {
//            f.
//            std::cout << this->filename << "\n";
//            auto is = seastar::make_file_input_stream(f);
//            while (true) {
//                auto data = co_await is.read();
//                lg.info("SSTable filename: {}", this->filename);
//                lg.info("Read {} bytes from {}", data.size(), this->filename);
//                if (data.empty())
//                    break;
//                //lg.info("data: {}", data.get());
//                std::string raw(data.get(), data.size());
//                std::vector<std::string> lines;
//                boost::algorithm::split(lines, raw, boost::is_any_of("\n"));
//                for (auto& line : lines) {
//                    std::istringstream iss(line);
//                    std::string curr_key, value;
//                    std::getline(iss, curr_key, ',');
//                    std::getline(iss, value);
//                    if (curr_key == key && value != deletion_marker)
//                        co_return value;
//                }
//            }
//            co_return std::nullopt;
//        });

//Second attempt
//seastar::future<std::optional<seastar::sstring>>
//SSTable::get(const seastar::sstring& key) const {
//    return seastar::util::with_file_input_stream(std::filesystem::path(filename),
//        [&key](seastar::input_stream<char>& is) -> seastar::future<std::optional<seastar::sstring>> {
//            lg.info("Key in search: {}", key);
//            while (true) {
//                auto data = co_await is.read();
//                //lg.info("SSTable filename: {}", filename);
//                //lg.info("Read {} bytes from {}", data.size(), filename);
//                if (data.empty())
//                    break;
//                lg.info("data: {}", data.get());
//                std::string raw(data.get(), data.size());
//                std::vector<std::string> lines;
//                boost::algorithm::split(lines, raw, boost::is_any_of("\n"));
//                for (auto& line : lines) {
//                    lg.info("Line: {}", line);
//                    std::istringstream iss(line);
//                    std::string curr_key, value;
//                    std::getline(iss, curr_key, ',');
//                    lg.info("Parsed key: {}", curr_key);
//                    std::getline(iss, value);
//                    lg.info("Parsed value: {}", value);
//                    if (curr_key == key) {
//                        if (value != "__deleted__")
//                            co_return value;
//                        else
//                            co_return std::nullopt;
//                    }
//                }
//            }
//            co_return std::nullopt;
//        });
//}

/*
 * Third attempt
 *
 * FIXME: Do not load the whole file in memory. Use pagination.
 */
seastar::future<std::optional<seastar::sstring>>
SSTable::get(const seastar::sstring& key) const {
    lg.info("Key in search: {}", key);
        //for (auto character : key)
        //    lg.info("{}", character);
        for (auto it = key.begin(); it <= key.end(); it++)
            lg.info("{}", static_cast<int>(*it));
    auto buf = co_await seastar::util::read_entire_file_contiguous(std::filesystem::path(filename));
    if (!buf.empty())
        lg.info("Data read:\n{}", buf);
    std::istringstream iss(buf);
    std::string line;
    while (std::getline(iss, line)) {
        std::istringstream iss(line);
        std::string curr_key, value;
        std::getline(iss, curr_key, ',');
        lg.info("Current key: {}", curr_key);
        for (auto it = curr_key.begin(); it <= curr_key.end(); it++)
            lg.info("{}", static_cast<int>(*it));
        //curr_key.erase(curr_key.find_last_not_of(" \n\r\t") + 1);
        //lg.info("Current key after whitespace removal: {}", curr_key);
        //for (auto it = curr_key.begin(); it <= curr_key.end(); it++)
        //    lg.info("{}", static_cast<int>(*it));
        //for (auto character : curr_key)
        //    lg.info("{}", character);
        std::getline(iss, value);
        lg.info("Current value: {}", value);
        //for (auto character : value)
        //    lg.info("{}", character);
        for (auto it = value.begin(); it <= value.end(); it++)
            lg.info("{}", static_cast<int>(*it));
        if (curr_key == key && value != deletion_marker) {
            lg.info("Found the key!");
            co_return value;
        }
    }
    lg.info("Did not find the key!");
    co_return std::nullopt;
}