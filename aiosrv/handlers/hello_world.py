
from aiohttp import web

async def hello(request):
    return web.Response(body=b"Hello, world")
