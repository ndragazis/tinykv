#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/thread.hh>

#include "stop_signal.hh"
#include "kvstore.hh"

namespace http = seastar::http;
namespace httpd = seastar::httpd;

seastar::logger lg(__FILE__);

void set_routes(httpd::routes& r, KVStore& store) {
    httpd::function_handler* read_key = new httpd::function_handler(
        [&store](httpd::const_req req, http::reply& rep) {
            auto key = req.get_path_param("key");
            auto value = store.get(key);
            if (value.has_value())
                return *value;
            rep.set_status(http::reply::status_type::not_found).done();
            return seastar::sstring();
    }, "json");
    httpd::function_handler* write_key = new httpd::function_handler(
        [&store](httpd::const_req req) {
            auto key = req.get_path_param("key");
            auto value = req.content;
            store.put(key, value);
            return value;
    });
    httpd::function_handler* delete_key = new httpd::function_handler(
        [&store](httpd::const_req req) {
            auto key = req.get_path_param("key");
            store.remove(key);
            return seastar::sstring();
    });
    r.add(httpd::operation_type::GET, httpd::url("/keys").remainder("key"), read_key);
    r.add(httpd::operation_type::PUT, httpd::url("/keys").remainder("key"), write_key);
    r.add(httpd::operation_type::DELETE, httpd::url("/keys").remainder("key"), delete_key);
}

seastar::future<> run_http_server() {
    seastar_apps_lib::stop_signal stop_signal;
    seastar::sstring ip = "127.0.0.1";
    uint16_t port = 9999;
    auto addr = seastar::make_ipv4_address({static_cast<std::string>(ip), port});
    httpd::http_server_control server;

    seastar::sstring dir = std::string(getenv("HOME")) + "/.tinykv";
    KVStore store(20, dir);

    lg.info("Starting HTTP server");
    co_await server.start();
    lg.info("Setting routes for HTTP server");
    co_await server.set_routes([&store](httpd::routes& r) {
        set_routes(r, store);
    });
    lg.info("Listening on HTTP address {}", addr);
    co_await server.listen(addr);
    lg.info("Use SIGINT or SIGTERM to stop the server");
    co_await stop_signal.wait();
    lg.info("Stopping HTTP server");
    co_await server.stop();
    lg.info("HTTP server stopped");
    co_return;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    return app.run(argc, argv, []() -> seastar::future<int> {
        co_await run_http_server();
        co_return 0;
    });
}
