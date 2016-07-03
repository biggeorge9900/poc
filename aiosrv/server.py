import argparse
from aiohttp import web
from aiosrv.handlers.hello_world import hello

ROUTES = [["GET", "/", hello]]


def init():
    app = web.Application()
    for route in ROUTES:
        app.router.add_route(*route)
    return app


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
    web.run_app(init(), host=args.host, port=args.port)

if __name__ == "__main__":
    main()
