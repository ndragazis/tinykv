#ifndef CACHE_HH
#define CACHE_HH
#include <map>
#include <list>
#include <optional>

#include <seastar/core/sstring.hh>

class Cache {
private:
    struct Node {
        seastar::sstring key;
        seastar::sstring value;
    };
    size_t capacity;
    std::unordered_map<seastar::sstring, std::list<Node>::iterator> map;
    std::list<Node> list;
public:
    Cache(size_t capacity);
    std::optional<seastar::sstring> get(const seastar::sstring& key);
    void put(const seastar::sstring& key, const seastar::sstring& value);
};


#endif  //CACHE_HH