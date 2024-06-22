#include <filesystem>
#include <iostream>

#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>

#include "kvstore.hh"
#include "file-utils.hh"

KVStore::KVStore(int memtable_size, const seastar::sstring& dir)
    : dir(dir)
    , wal_filename(dir + "/wal")
    , flush_threshold(memtable_size)
    , wal_index(0)
    , sstable_index(0)
{
    std::cout << "KVStore - Creating directory " << dir << "\n";
    std::filesystem::create_directories(std::filesystem::path(dir));
    recover_active_memtables();
    current_memtable = std::make_unique<MemTable>(wal_filename);
    load_sstables();
}

std::optional<seastar::sstring> KVStore::get(const seastar::sstring& key) const {
    std::cout << "Searcing for key " << key << " in current memtable\n";
    auto value = current_memtable->get(key);
    if (value.has_value()) {
        return value;
    }
    for (const auto& memtable : active_memtables) {
        std::cout << "Searcing for key " << key << " in active memtable " << memtable->wal.filename << "\n";
        value = memtable->get(key);
        if (value.has_value()) {
            return value;
        }
    }
    for (const auto& sstable : sstables) {
        std::cout << "Searcing for key " << key << " in sstable " << sstable.filename << "\n";
        value = sstable.get(key);
        if (value.has_value()) {
            return value;
        }
    }
    return std::nullopt;
}

void KVStore::put(const seastar::sstring& key, const seastar::sstring& value) {
    if (current_memtable->size() > flush_threshold) {
        create_new_memtable();
    }
    current_memtable->put(key, value);
}

std::optional<seastar::sstring> KVStore::remove(const seastar::sstring& key) {
    return current_memtable->remove(key);
}

void KVStore::create_new_memtable() {
    std::cout << "Creating new memtable...\n";
    // Rotate WAL file
    std::string old_wal_filename = current_memtable->wal.filename;
    std::string new_wal_filename = dir + "/wal_" + std::to_string(++wal_index);

    std::rename(old_wal_filename.c_str(), new_wal_filename.c_str());
    current_memtable->wal.filename = new_wal_filename;

    auto new_memtable = std::make_unique<MemTable>(dir + "/wal");

    // Swap the new memtable with the current one
    std::shared_ptr<MemTable> old_memtable = std::move(current_memtable);
    active_memtables.push_front(old_memtable);
    current_memtable = std::move(new_memtable);

    // Flush the old memtable to an SSTable asynchronously
    // FIXME: Only start a flush if this is the first in the list of active memtables
    flush_memtable(std::move(old_memtable)).handle_exception([](std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            std::cout << "Exception during flushing: " << e.what() << "\n";
        }
    }).get();
}

//seastar::future<> KVStore::flush_memtable(std::shared_ptr<MemTable> old_memtable) {
//    // Generate a unique filename for the SSTable
//    std::string sstable_filename = dir + "/sstable";
//    // Open the file for writing
//    return seastar::open_file_dma(sstable_filename, seastar::open_flags::rw | seastar::open_flags::create)
//    .then([old_memtable = std::move(old_memtable)](seastar::file f) {
//        return seastar::do_with(std::move(f), [old_memtable = std::move(old_memtable)](seastar::file& f) {
//            // Write the contents of the old memtable to the SSTable
//            return seastar::do_for_each(old_memtable->table.begin(), old_memtable->table.end(), [&f](const auto& kv) {
//                auto entry = kv.first + " " + kv.second + "\n";
//                return f.dma_write(entry.data(), entry.size());
//            }).then([&f] {
//                // Close the file
//                return f.flush().then([&f] {
//                    return f.close();
//                });
//            });
//        });
//    }).then([old_memtable](seastar::future<> f) mutable {
//        try {
//            f.get();
//        } catch (...) {
//            // Handle exception during the file operation
//            seastar::print("Error during SSTable creation: %s\n", std::current_exception());
//        }
//
//        // Remove the old WAL file
//        std::string wal_filename = old_memtable->wal.filename;
//        if (std::filesystem::exists(wal_filename)) {
//            std::remove(wal_filename.c_str());
//        }
//
//        return seastar::make_ready_future<>();
//    });
//}
seastar::future<> KVStore::flush_memtable(std::shared_ptr<MemTable> old_memtable) {
    std::string sstable_filename = dir + "/sstable_" + std::to_string(++sstable_index);
    std::ofstream sstable_file(sstable_filename);
    if (!sstable_file.is_open()) {
        throw std::runtime_error("Failed to open SSTable file for writing");
    }
    for (const auto& kv : old_memtable->_map) {
        sstable_file << kv.first << "," << kv.second << "\n";
    }

    sstable_file.close();

    // Remove the old WAL file
    std::string wal_filename = old_memtable->wal.filename;
    if (std::filesystem::exists(wal_filename)) {
        std::remove(wal_filename.c_str());
    }
    active_memtables.remove(old_memtable);
    sstables.emplace_back(sstable_filename);
    return seastar::make_ready_future<>();
}

void KVStore::recover_active_memtables() {
    namespace fs = std::filesystem;
    //Find WALs.
    const std::string wal_prefix = "wal_";
    auto files = find_files(dir, wal_prefix);
    std::sort(files.begin(), files.end(),
    [&wal_prefix, this](const std::string& a, const std::string& b) {
        int numA = extractNumber(fs::path(a).filename().string(), wal_prefix);
        int numB = extractNumber(fs::path(b).filename().string(), wal_prefix);
        if (numA > this->wal_index) this->wal_index = numA;
        if (numB > this->wal_index) this->wal_index = numB;
        return numA > numB;
    });
    //Recover WALs into memtables and initiate flushing.
    for (const auto& file : files) {
        std::cout << "Found WAL " << file << "\n";
        std::shared_ptr<MemTable> memtable = std::make_shared<MemTable>(file);
        active_memtables.push_back(std::move(memtable));
        //flush_memtable(std::move(memtable));
    }
    std::cout << "WAL index: " << wal_index << "\n";
}

void KVStore::load_sstables() {
    namespace fs = std::filesystem;
    const std::string prefix = "sstable_";
    auto files = find_files(dir, prefix);
    std::sort(files.begin(), files.end(),
    [&prefix, this](const std::string& a, const std::string& b) {
        int numA = extractNumber(fs::path(a).filename().string(), prefix);
        int numB = extractNumber(fs::path(b).filename().string(), prefix);
        if (numA > this->sstable_index) this->sstable_index = numA;
        if (numB > this->sstable_index) this->sstable_index = numB;
        return numA > numB;
    });
    for (const auto& file : files) {
        std::cout << "Found SSTable " << file << "\n";
        sstables.emplace_back(file);
    }
    std::cout << "SSTable index: " << sstable_index << "\n";
}