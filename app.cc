#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>

using namespace seastar;

logger applog(__FILE__);

int main(int argc, char** argv) {
    app_template app;

    return app.run(argc, argv, [&app] {
        applog.info("HELLO WORLD");
        return make_ready_future<>();
    });
}
