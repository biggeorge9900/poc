import asyncio
import asyncio_redis


def catch_redis_errors(fn):
    def cancel_future(future):
        future.cancel()

    async def error_wrapper(instance, *args, **kwargs):
        count = 0
        while count < 5:
            print("****{}".format(count))
            try:
                f = asyncio.ensure_future(fn(instance, *args, **kwargs))
                asyncio.get_event_loop().call_later(5, cancel_future, f)
                return await f
            except (asyncio_redis.NoAvailableConnectionsInPoolError,
                    asyncio.streams.IncompleteReadError,
                    asyncio.CancelledError):
                print("ERROR ****{}".format(count))
                await asyncio.sleep(1)
                f = asyncio.ensure_future(instance.initialize(reset=True))
                asyncio.get_event_loop().call_later(2, cancel_future, f)
                try:
                    await f
                except asyncio.CancelledError:
                    pass

                count += 1
                if count > 4:
                    return None
        print("Hello?")
        return None

    return error_wrapper


class Redis(object):
    _instance = None
    _initialized = False

    def __new__(cls):
        if not cls._instance:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self._connection = None

    async def initialize(self, reset=False):
        if reset or not self._connection:
            if self._connection:
                self._connection.close()

            self._connection = await asyncio_redis.Pool.create(host='127.0.0.1',
                                                               port=6379,
                                                               poolsize=10,
                                                               auto_reconnect=False)

    @catch_redis_errors
    async def get(self, key):
        return await self._connection.get(key)

    async def set(self, key, value):
        return await self._connection.set(key, value)

    def close_connections(self):
        if self._connection:
            self._connection.close()
