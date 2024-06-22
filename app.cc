#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/thread.hh>

#include "stop_signal.hh"
#include "kvstore.hh"

using namespace seastar;

logger applog(__FILE__);

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

future<int> run_http_server() {
    return async([] {
        seastar_apps_lib::stop_signal stop_signal;
        net::inet_address addr("127.0.0.1");
        uint16_t port = 9999;
        httpd::http_server_control server;

        seastar::sstring dir = seastar::sstring(getenv("HOME")) + "/.tinykv";
        KVStore store(64, dir);

        std::cout << "starting HTTP server" << std::endl;
        server.start().get();
        std::cout << "setting routes for HTTP server" << std::endl;
        server.set_routes([&store](httpd::routes& r) {
            set_routes(r, store);
        }).get();
        std::cout << "listening on HTTP server" << std::endl;
        server.listen(port).get();
        std::cout << "HTTP server listening on port " << port << " ...\n";
        std::cout << "stopping HTTP server" << std::endl;
        stop_signal.wait().get();
        server.stop().get();
        std::cout << "HTTP server stopped" << std::endl;
        return 0;
    });
}

int main(int argc, char** argv) {
    app_template app;

    return app.run(argc, argv, run_http_server);
}
