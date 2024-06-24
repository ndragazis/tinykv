#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/thread.hh>
#include <seastar/json/json_elements.hh>

#include "stop_signal.hh"
#include "kvstore.hh"

namespace http = seastar::http;
namespace httpd = seastar::httpd;

static seastar::logger lg(__FILE__);

void set_routes(httpd::routes& r, KVStore& store) {
    httpd::function_handler* read_key = new httpd::function_handler(
        [&store](std::unique_ptr<http::request> req,
                 std::unique_ptr<http::reply> rep) {
            auto key = req->get_path_param("key");
            return store.get(key).then([&](auto value) {
            if (value.has_value()) {
                rep->set_status(http::reply::status_type::ok);
                rep->write_body("json", std::string(value.value()));
            } else {
                rep->set_status(http::reply::status_type::not_found).done();
            }
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        });
    }, "json");
    httpd::function_handler* write_key = new httpd::function_handler(
        [&store](std::unique_ptr<http::request> req) {
            auto key = req->get_path_param("key");
            auto value = req->content;
            return store.put(key, value).then([&]() {
                return seastar::make_ready_future<seastar::json::json_return_type>(value);
            });
    });
    httpd::function_handler* delete_key = new httpd::function_handler(
        [&store](std::unique_ptr<http::request> req,
                 std::unique_ptr<http::reply> rep) {
            auto key = req->get_path_param("key");
            return store.remove(key).then([&]() {
                rep->set_status(http::reply::status_type::ok);
                return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
            });
    }, "json");
    r.add(httpd::operation_type::GET, httpd::url("/keys").remainder("key"), read_key);
    r.add(httpd::operation_type::PUT, httpd::url("/keys").remainder("key"), write_key);
    r.add(httpd::operation_type::DELETE, httpd::url("/keys").remainder("key"), delete_key);
}

seastar::future<int> run_http_server(KVStore& store) {
    seastar_apps_lib::stop_signal stop_signal;
    seastar::sstring ip = "127.0.0.1";
    uint16_t port = 9999;
    auto addr = seastar::make_ipv4_address({static_cast<std::string>(ip), port});
    httpd::http_server_control server;

    std::exception_ptr eptr;
    try {
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
    } catch(...) {
        eptr = std::current_exception();
    }
    lg.info("Stopping HTTP server");
    co_await server.stop();
    lg.info("HTTP server stopped");
    if (eptr) {
        co_return seastar::coroutine::exception(eptr);
    }
    co_return 0;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    return app.run(argc, argv, []() -> seastar::future<int> {
        seastar::sstring dir = std::string(getenv("HOME")) + "/.tinykv";
        KVStore store(20, dir);

        co_await store.start();
        std::exception_ptr eptr;
        try {
            co_await run_http_server(store);
        } catch(...) {
            eptr = std::current_exception();
        }
        co_await store.stop();
        if (eptr) {
            co_return seastar::coroutine::exception(eptr);
        }
        co_return 0;
    });
}
