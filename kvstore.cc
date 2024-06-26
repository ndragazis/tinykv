#include <filesystem>
#include <iostream>

#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>

#include "kvstore.hh"
#include "file-utils.hh"

static seastar::logger lg(__FILE__);

KVStore::KVStore(int memtable_size, int cache_capacity, const seastar::sstring& dir)
    : dir(dir + "/shard_" + std::to_string(seastar::this_shard_id()))
    , wal_filename(dir + "/shard_" + std::to_string(seastar::this_shard_id()) + "/wal")
    , flush_threshold(memtable_size)
    , wal_index(0)
    , sstable_index(0)
    , cache(cache_capacity)
{}

seastar::future<> KVStore::start() {
    lg.debug("Starting KVStore");
    lg.info("Creating directory {}", dir);
    co_await seastar::recursive_touch_directory(dir);
    co_await recover_active_memtables();
    current_memtable = std::make_unique<MemTable>(wal_filename);
    co_await current_memtable->load();
    co_await load_sstables();
    co_return;
}

seastar::future<> KVStore::stop() noexcept {
    lg.debug("Stopping KVStore");
    co_return co_await flush_memtable(std::move(current_memtable));
}

seastar::future<std::optional<seastar::sstring>>
KVStore::get(const seastar::sstring& key) {
    lg.info("Searcing for key {}", key);
    lg.debug("Searcing for key {} in cache", key);
    auto value = cache.get(key);
    if (value.has_value()) {
        co_return (value.value() == MemTable::deletion_marker ||
                   value.value() == SSTable::deletion_marker) ? std::nullopt : value;
    }
    lg.debug("Searcing for key {} in current memtable (wal: {})", key, current_memtable->wal.filename);
    value = current_memtable->get(key);
    if (value.has_value()) {
        cache.put(key, value.value());
        co_return (value.value() == MemTable::deletion_marker) ? std::nullopt : value;
    }
    for (const auto& memtable : active_memtables) {
        lg.debug("Searcing for key {} in active memtable (wal: {})", key, memtable->wal.filename);
        value = memtable->get(key);
        if (value.has_value()) {
            cache.put(key, value.value());
            co_return (value.value() == MemTable::deletion_marker) ? std::nullopt : value;
        }
    }
    for (const auto& sstable : sstables) {
        lg.debug("Searcing for key {} in sstable {}", key, sstable.filename);
        value = co_await sstable.get(key);
        if (value.has_value()) {
            cache.put(key, value.value());
            co_return (value.value() == SSTable::deletion_marker) ? std::nullopt : value;
        }
    }
    co_return std::nullopt;
}

seastar::coroutine::experimental::generator<std::pair<seastar::sstring, seastar::sstring>>
KVStore::get() const {
    std::map<seastar::sstring, seastar::sstring> data;
    lg.debug("Getting all keys from current memtable (wal: {})", current_memtable->wal.filename);
    for (const auto& [key, value] : *current_memtable) {
        data.emplace(key, value.value());
    }
    for (const auto& memtable : active_memtables) {
        lg.debug("Getting all keys from active memtale (wal: {})", memtable->wal.filename);
        for (const auto& [key, value] : *memtable) {
            if (data.find(key) != data.end())
                continue;
            data.emplace(key, value.value());
        }
    }
    for (const auto& sstable : sstables) {
        lg.debug("Getting all keys from sstable {}", sstable.filename);
        auto kvs = sstable.get();
        while (const auto& kv = co_await kvs()) {
            auto [key, value] = kv.value();
            if (data.find(key) != data.end())
                continue;
            data.emplace(key, value);
        }
    }
    for (const auto& [key, value] : data) {
        if (value != MemTable::deletion_marker &&
                value != SSTable::deletion_marker)
            co_yield std::pair{key, value};
    }
}

seastar::future<> KVStore::put(const seastar::sstring& key, const seastar::sstring& value) {
    if (current_memtable->size() > flush_threshold) {
        co_await create_new_memtable();
    }
    cache.put(key, value);
    co_await current_memtable->put(key, value);
    co_return;
}

seastar::future<> KVStore::remove(const seastar::sstring& key) {
    cache.put(key, MemTable::deletion_marker);
    co_await current_memtable->remove(key);
    co_return;
}

seastar::future<> KVStore::create_new_memtable() {
    lg.info("Creating new memtable...");
    // Rotate WAL file
    std::string old_wal_filename = current_memtable->wal.filename;
    std::string new_wal_filename = dir + "/wal_" + std::to_string(++wal_index);

    co_await seastar::rename_file(old_wal_filename, new_wal_filename);
    co_await seastar::sync_directory(dir);
    current_memtable->wal.filename = new_wal_filename;

    auto new_memtable = std::make_unique<MemTable>(dir + "/wal");
    co_await new_memtable->load();

    // Swap the new memtable with the current one
    std::shared_ptr<MemTable> old_memtable = std::move(current_memtable);
    active_memtables.push_front(old_memtable);
    current_memtable = std::move(new_memtable);

    // Flush the old memtable to an SSTable
    co_return co_await flush_memtable(std::move(old_memtable));
}

seastar::future<> KVStore::flush_memtable(std::shared_ptr<MemTable> old_memtable) {
    if (old_memtable->size() == 0) {
        lg.debug("Skip flushing for empty memtable (wal: {}).",
            old_memtable->wal.filename);
        co_return co_await delete_memtable(old_memtable);
    }

    std::string sstable_filename = dir + "/sstable_" + std::to_string(++sstable_index);
    lg.info("Flushing memtable (wal: {}) into SSTable: {}",
        old_memtable->wal.filename, sstable_filename);

    /* Code based on `seastar/demos/file_demo.cc`. */

    auto file_flags = seastar::open_flags::rw | seastar::open_flags::create;
    auto f = seastar::open_file_dma(sstable_filename, file_flags);
    auto os = co_await seastar::with_file_close_on_failure(std::move(f),
        [] (seastar::file f) -> seastar::future<seastar::output_stream<char>> {
            return seastar::make_file_output_stream(std::move(f));
    });

    co_await seastar::do_for_each(old_memtable->begin(), old_memtable->end(),
        [&] (std::pair<const seastar::sstring, std::optional<seastar::sstring>> & kv) -> seastar::future<> {
            if (kv.second.has_value())
                return os.write(kv.first + "," + kv.second.value() + "\n");
            else
                return os.write(kv.first + "," + SSTable::deletion_marker + "\n");
    }).finally([&os] {return os.close();});

    sstables.emplace_back(sstable_filename);
    co_return co_await delete_memtable(old_memtable);
}

seastar::future<> KVStore::delete_memtable(std::shared_ptr<MemTable> memtable) {
    std::string wal_filename = memtable->wal.filename;
    if (co_await seastar::file_exists(wal_filename)) {
        co_await seastar::remove_file(wal_filename);
        co_await seastar::sync_directory(dir);
    }
    active_memtables.remove(memtable);
    co_await memtable->destroy();
}

seastar::future<> KVStore::recover_active_memtables() {
    lg.info("Recovering active memtables from disk");
    namespace fs = std::filesystem;
    //Find WALs.
    const std::string wal_prefix = "wal_";
    auto files = co_await find_files(dir, wal_prefix);
    std::sort(files.begin(), files.end(),
    [&wal_prefix, this](const std::string& a, const std::string& b) {
        int numA = extractNumber(fs::path(a).filename().string(), wal_prefix);
        int numB = extractNumber(fs::path(b).filename().string(), wal_prefix);
        return numA > numB;
    });
    if (!files.empty())
        wal_index = extractNumber(fs::path(files[0]).filename().string(), wal_prefix);
    //Recover WALs into memtables and initiate flushing.
    for (const auto& file : files) {
        lg.debug("Found WAL: {} ", file);
        std::shared_ptr<MemTable> memtable = std::make_shared<MemTable>(file);
        co_await memtable->load();
        active_memtables.push_back(std::move(memtable));
        //flush_memtable(std::move(memtable));
    }
    lg.debug("WAL index: {}", wal_index);
}

seastar::future<> KVStore::load_sstables() {
    lg.info("Finding SSTables from disk");
    namespace fs = std::filesystem;
    const std::string prefix = "sstable_";
    auto files = co_await find_files(dir, prefix);
    std::sort(files.begin(), files.end(),
    [&prefix, this](const std::string& a, const std::string& b) {
        int numA = extractNumber(fs::path(a).filename().string(), prefix);
        int numB = extractNumber(fs::path(b).filename().string(), prefix);
        return numA > numB;
    });
    if (!files.empty())
        sstable_index = extractNumber(fs::path(files[0]).filename().string(), prefix);
    for (const auto& file : files) {
        lg.debug("Found SSTable: {} ", file);
        sstables.emplace_back(file);
    }
    lg.debug("SSTable index: {}", sstable_index);
    co_return;
}