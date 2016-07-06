import argparse
from aiohttp import web
from aiosrv.handlers.hello_world import hello
from aiosrv.redis import Redis
from potato.mainstream import AioMainStream, AioMainStreamQueue

ROUTES = [["GET", "/", hello]]


class MyServer(object):

    def init(self):
        app = web.Application()
        for route in ROUTES:
            app.router.add_route(*route)
        app.on_shutdown.append(self.on_shutdown)
        app.loop.run_until_complete(self.on_startup())
        return app

    async def on_startup(self):
        await Redis().initialize()
        AioMainStream().initialize([("localhost", 6379)])
        await AioMainStreamQueue("testq1").enqueue("TestValue1:Hello!", expires=1)
        return await Redis().set("mytest", "Hola World!")

    async def on_shutdown(self, app):
        Redis().close_connections()
        AioMainStream().close_connections()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host",
                   dest="host",
                   default="0.0.0.0",
                   required=False)
    p.add_argument("--port",
                   dest="port",
                   default=8080,
                   required=False)
    args = p.parse_args()
    srv = MyServer()
    web.run_app(srv.init(), host=args.host, port=args.port)

if __name__ == "__main__":
    main()
