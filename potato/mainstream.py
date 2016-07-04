import logging
import hashlib
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
        self.connection_cache = {}

    def clear_connection_cache(self):
        self.connection_cache = {}

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
                    try:
                        conn.delete(vid)
                    except:
                        pass

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
