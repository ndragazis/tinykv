#include <seastar/util/log.hh>

#include "cache.hh"

static seastar::logger lg(__FILE__);

Cache::Cache(size_t capacity)
    : capacity(capacity), map(), list()
{}

std::optional<seastar::sstring> Cache::get(const seastar::sstring& key) {
    lg.debug("Getting key {} from cache", key);
    auto find_it = map.find(key);
    if (find_it == map.end()) {
        lg.debug("Did not find key {} in cache", key);
        return std::nullopt;
    }
    lg.debug("Found the key!");
    auto list_it = find_it->second;
    auto node = std::move(*list_it);
    list.erase(list_it);
    list.emplace_back(std::move(node));
    return list.back().value;
}

void Cache::put(const seastar::sstring& key, const seastar::sstring& value) {
    lg.debug("Putting key {} in cache", key);
    auto find_it = map.find(key);
    if (find_it != map.end()) {
        lg.debug("Key already in cache. Updating the LRU status");
        auto list_it = find_it->second;
        list_it->value = value;
        auto node = std::move(*list_it);
        list.erase(list_it);
        list.emplace_back(std::move(node));
        return;
    }
    lg.debug("Did not find key {} in cache. Inserting it", key);
    if (list.size() == capacity) {
        auto last = list.end();
        --last;
        lg.debug("Cache capacity is full. Evicting key {}", last->key);
        map.erase(last->key);
        list.pop_back();
    }
    list.emplace_front(key, value);
    map[key] = list.begin();
}