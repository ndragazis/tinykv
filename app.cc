#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/thread.hh>

#include "stop_signal.hh"

using namespace seastar;

logger applog(__FILE__);

void set_routes(httpd::routes& r) {
    httpd::function_handler* read_key = new httpd::function_handler(
        [](httpd::const_req req) {
            auto key = req.get_path_param("key");
            return key;
    });
    r.add(httpd::operation_type::GET, httpd::url("/keys").remainder("key"), read_key);
}

future<int> run_http_server() {
    return async([] {
        seastar_apps_lib::stop_signal stop_signal;
        net::inet_address addr("127.0.0.1");
        uint16_t port = 9999;
        httpd::http_server_control server;
        std::cout << "starting HTTP server" << std::endl;
        server.start().get();
        std::cout << "setting routes for HTTP server" << std::endl;
        server.set_routes(set_routes).get();
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
