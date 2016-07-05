
from aiohttp import web
from aiosrv.redis import Redis


async def hello(request):
    return web.Response(body="Hello/{}/".format(await Redis().get("mytest")).encode())
