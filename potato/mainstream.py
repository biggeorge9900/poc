import logging
import hashlib
import asyncio_redis
import uuid
import json
from redis import Redis
from redis.connection import (ConnectionPool,
                              ConnectionError)

logger = logging.getLogger(__name__)


class RedisConnectionMaker(object):
    CONNECTION_POOL = {}

    def __init__(self, redis_shards):
        assert type(redis_shards) in [list, tuple]
        num_of_shards = len(redis_shards)

        def get_connection_single_fn():
            host = redis_shards[0][0]
            port = redis_shards[0][1]

            def get_connection_single(qname):
                return self.make_connection(host, port)

            return get_connection_single

        def get_connection_by_qname_fn():
            queue_to_shard_map = {}

            def get_connection(qname):
                if qname not in queue_to_shard_map:
                    shard = int(int(str(hashlib.sha1(qname).hexdigest()), 16) % num_of_shards)
                    queue_to_shard_map[qname] = shard
                else:
                    shard = queue_to_shard_map[qname]

                host = redis_shards[shard][0]
                port = redis_shards[shard][1]
                return self.make_connection(host, port)

            return get_connection

        if num_of_shards == 0:
            raise Exception("No redis shards defined")
        elif num_of_shards == 1:
            self.get_connection = get_connection_single_fn()
        else:
            self.get_connection = get_connection_by_qname_fn()

        self.pool_max_connection = int(1000 / num_of_shards)

    def make_connection(self, host, port):
        connection_id = "{}:{}".format(host, port)
        if connection_id not in self.CONNECTION_POOL:
            logger.info("Connection: %s:%s added.", host, port)
            self.CONNECTION_POOL[connection_id] = ConnectionPool(max_connections=self.pool_max_connection,
                                                                 host=host,
                                                                 port=port)
        pool = self.CONNECTION_POOL[connection_id]
        return Redis(connection_pool=pool)

    def __call__(self, qname):
        return self.get_connection(qname)


def catch_connection_error(on_error_return):
    def decorator(fn):
        def connection_error_handler(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except ConnectionError as e:
                logger.error("ConnectionError: %s", repr(e))
                return on_error_return

        return connection_error_handler

    return decorator


class MainStream(object):
    _instance = None
    CONNECTION_MAKER = None
    QSET_NAME = "mainstream-queues"
    QKEY_TMPL = "q:{}"

    def __new__(cls):
        if not cls._instance:
            cls._instance = object.__new__(cls)
        return cls._instance

    def initialize(self, redis_shards=None):
        if redis_shards is None:
            redis_shards = []
        self.CONNECTION_MAKER = RedisConnectionMaker(redis_shards)

    @catch_connection_error(on_error_return=False)
    def is_queue_exists(self, queue_name):
        conn = self.CONNECTION_MAKER(queue_name)
        return conn.sismember(self.QSET_NAME, queue_name)

    @catch_connection_error(on_error_return=False)
    def enqueue(self, queue_name, item, expires=30):
        conn = self.CONNECTION_MAKER(queue_name)
        conn.sadd(self.QSET_NAME, queue_name)
        vid = str(uuid.uuid1())
        conn.setex(vid, json.dumps(item), expires)
        conn.rpush(self.QKEY_TMPL.format(queue_name), vid)
        return True

    @catch_connection_error(on_error_return=None)
    def dequeue(self, queue_name, timeout=10, no_wait=False):
        conn = self.CONNECTION_MAKER(queue_name)
        qnamekey = self.QKEY_TMPL.format(queue_name)
        while True:
            if no_wait:
                vid = conn.lpop(qnamekey)
            else:
                vid = conn.blpop(qnamekey, timeout=timeout)

            vid = vid[1] if vid else None

            if not vid:
                return None
            elif conn.exists(vid):
                try:
                    return json.loads(conn.get(vid))
                finally:
                    conn.delete([vid])

            if conn.llen(qnamekey) > 100:
                # over 100 message in the queue
                # probably backed up. reset itMAX_CONNECTIONS
                self.reset_queue(queue_name)
            else:
                logger.warning("Message: %s expired", vid)

    @catch_connection_error(on_error_return=None)
    def queue_length(self, queue_name):
        conn = self.CONNECTION_MAKER(queue_name)
        return conn.llen(self.QKEY_TMPL.format(queue_name))

    @catch_connection_error(on_error_return=False)
    def delete_queue(self, queue_name):
        conn = self.CONNECTION_MAKER(queue_name)
        conn.delete(self.QKEY_TMPL.format(queue_name))
        conn.srem(self.QSET_NAME, queue_name)

    @catch_connection_error(on_error_return=False)
    def reset_queue(self, queue_name):
        conn = self.CONNECTION_MAKER(queue_name)
        conn.delete(self.QKEY_TMPL.format(queue_name))

    @catch_connection_error(on_error_return=None)
    def peek_queue(self, queue_name):
        conn = self.CONNECTION_MAKER(queue_name)
        qkey = self.QKEY_TMPL.format(queue_name)
        conn.lrange(qkey, 0, conn.llen(qkey))


class AioRedisConnectionMaker(object):
    CONNECTION_POOL = {}

    def __init__(self, redis_shards):
        assert type(redis_shards) in [list, tuple]
        num_of_shards = len(redis_shards)

        def get_connection_single_fn():
            host = redis_shards[0][0]
            port = redis_shards[0][1]

            async def get_connection_single(qname):
                return await self.make_connection(host, port)

            return get_connection_single

        def get_connection_by_qname_fn():
            queue_to_shard_map = {}

            async def get_connection(qname):
                if qname not in queue_to_shard_map:
                    shard = int(int(str(hashlib.sha1(qname).hexdigest()), 16) % num_of_shards)
                    queue_to_shard_map[qname] = shard
                else:
                    shard = queue_to_shard_map[qname]

                host = redis_shards[shard][0]
                port = redis_shards[shard][1]
                return await self.make_connection(host, port)

            return get_connection

        if num_of_shards == 0:
            raise Exception("No redis shards defined")
        elif num_of_shards == 1:
            self.get_connection = get_connection_single_fn()
        else:
            self.get_connection = get_connection_by_qname_fn()

        self.pool_max_connection = int(5 / num_of_shards)

    async def make_connection(self, host, port):
        connection_id = "{}:{}".format(host, port)
        if connection_id not in self.CONNECTION_POOL:
            logger.info("Connection: %s:%s added.", host, port)
            self.CONNECTION_POOL[connection_id] = await asyncio_redis.Pool.create(
                host=host,
                port=int(port),
                poolsize=self.pool_max_connection,
                auto_reconnect=False)
        return self.CONNECTION_POOL[connection_id]

    async def __call__(self, qname):
        return await self.get_connection(qname)

    def close_connections(self):
        for pool in self.CONNECTION_POOL.values():
            pool.close()


def aio_connection_wrapper(on_error_return):
    def decorator(fn):
        async def aio_wrapper(cls_inst,
                              queue_name,
                              *args,
                              **kwargs):
            try:
                redis = await cls_inst.CONNECTION_MAKER(queue_name)
                return await fn(cls_inst, redis, queue_name, *args, **kwargs)
            except ConnectionError as e:
                logger.error("ConnectionError: %s", repr(e))
                return on_error_return

        return aio_wrapper

    return decorator


class AioMainStream(object):
    _instance = None
    CONNECTION_MAKER = None
    QSET_NAME = "mainstream-queues"
    QKEY_TMPL = "q:{}"

    def __new__(cls):
        if not cls._instance:
            cls._instance = object.__new__(cls)
        return cls._instance

    def initialize(self, redis_shards=None):
        if redis_shards is None:
            redis_shards = []
        self.CONNECTION_MAKER = AioRedisConnectionMaker(redis_shards)

    def close_connections(self):
        self.CONNECTION_MAKER.close_connections()

    @aio_connection_wrapper(on_error_return=False)
    async def is_queue_exists(self, redis, queue_name):
        return await redis.sismember(self.QSET_NAME, queue_name)

    @aio_connection_wrapper(on_error_return=False)
    async def enqueue(self, redis, queue_name, item, expires=30):
        vid = str(uuid.uuid1())
        trans = await redis.multi()
        await trans.sadd(self.QSET_NAME, [queue_name])
        await trans.setex(vid, expires, json.dumps(item))
        await trans.rpush(self.QKEY_TMPL.format(queue_name), [vid])
        await trans.exec()
        return True

    @aio_connection_wrapper(on_error_return=None)
    async def dequeue(self, redis, queue_name, timeout=10, no_wait=False):
        qnamekey = self.QKEY_TMPL.format(queue_name)
        while True:
            if no_wait:
                vid = await redis.lpop(qnamekey)
            else:
                try:
                    reply = await redis.blpop([qnamekey], timeout=timeout)
                except asyncio_redis.exceptions.TimeoutError:
                    reply = None

                vid = reply.value if reply else None

            if not vid:
                return None
            elif await redis.exists(vid):
                try:
                    return json.loads(await redis.get(vid))
                finally:
                    await redis.delete([vid])

            if await redis.llen(qnamekey) > 100:
                # over 100 message in the queue
                # probably backed up. reset itMAX_CONNECTIONS
                self.reset_queue(queue_name)
            else:
                logger.warning("Message: %s expired", vid)

    @aio_connection_wrapper(on_error_return=None)
    async def queue_length(self, redis, queue_name):
        return redis.llen(self.QKEY_TMPL.format(queue_name))

    @aio_connection_wrapper(on_error_return=False)
    async def delete_queue(self, redis, queue_name):
        trans = await redis.multi()
        await trans.delete(self.QKEY_TMPL.format(queue_name))
        await trans.srem(self.QSET_NAME, queue_name)
        await trans.exec()

    @aio_connection_wrapper(on_error_return=False)
    async def reset_queue(self, redis, queue_name):
        await redis.delete(self.QKEY_TMPL.format(queue_name))

    @aio_connection_wrapper(on_error_return=None)
    async def peek_queue(self, redis, queue_name):
        qkey = self.QKEY_TMPL.format(queue_name)
        await redis.lrange(qkey, 0, await redis.llen(qkey))


class AioMainStreamQueue(object):
    def __init__(self, queue_name):
        self._qname = queue_name
        self._mainstream = AioMainStream()
        assert self._mainstream.CONNECTION_MAKER, "Connection maker not initialized"

    async def enqueue(self, item, expires=30):
        await self._mainstream.enqueue(self._qname, item, expires)

    async def dequeue(self, timeout=10, no_wait=False):
        return await self._mainstream.dequeue(self._qname, timeout, no_wait)
