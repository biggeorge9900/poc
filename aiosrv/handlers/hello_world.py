
from aiohttp import web
from aiosrv.redis import Redis
from potato.mainstream import AioMainStreamQueue


async def hello(request):
    # await Redis().delete(["key-does-not-exists"])
    # return web.Response(body="Hello/{}/".format(await Redis().get("mytest")).encode())
    return web.Response(body="Hello/{}/".format(await AioMainStreamQueue("testq1").dequeue()).encode())
