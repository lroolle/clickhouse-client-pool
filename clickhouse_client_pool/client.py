import itertools
import logging
import threading

from clickhouse_driver import Client

logger = logging.getLogger()


class BlockedClient(Client):
    """Block Client when execute"""

    def __init__(self, *args, **kwargs):
        super(BlockedClient, self).__init__(*args, **kwargs)
        self._lock = threading.Lock()

    def execute(self, *args, **kwargs):
        with self._lock:
            return super(BlockedClient, self).execute(*args, **kwargs)

    def execute_iter(self, *args, **kwargs):
        with self._lock:
            return super(BlockedClient, self).execute_iter(*args, **kwargs)

    @property
    def connected(self):
        return self.connection.connected

    @property
    def blocked(self):
        return self._lock.locked()


class ClickHouseClientPool(object):
    """Thread safe client connections pool
    config.xml: <max_connections>4096</max_connections>
    """

    def __init__(self, host, port=9000, max_connections=10, **kwargs):
        self.connection_args = {"host": host, "port": port, **kwargs}
        self.max_connections = max_connections
        self.closed = False
        self._pool = list()
        self._client_used = dict()
        self._lock = threading.Lock()
        self._pool.append(self.pull())

    def execute(self, sql, with_column_types=True, *args, **kwargs):
        client = self.pull()
        try:
            res = client.execute(
                sql, with_column_types=with_column_types, *args, **kwargs
            )
            self.push(client)
            return res
        except Exception as e:
            logger.exception("ClickHouse BlockedClient execute with exception: %s" % e)
            self._del_client_used(client)
        return []

    def execute_iter(self, sql, with_column_types=True, *args, **kwargs):
        client = self.pull()
        try:
            for rec in client.execute_iter(
                sql, with_column_types=with_column_types, *args, **kwargs
            ):
                yield rec
            self.push(client)
        except Exception as e:
            logger.exception(
                "ClickHouse BlockedClient execute_iter with exception: %s" % e
            )
            self._del_client_used(client)
            yield []

    def push(self, client):
        with self._lock:
            if client.connected and not client.blocked:
                self._pool.append(client)

    def pull(self):
        while len(self._client_used) >= self.max_connections:
            for client in itertools.cycle(self._client_used.values()):
                if len(self._pool) > 0:
                    client = self._pool.pop()
                if self._is_ready(client):
                    return client

        with self._lock:
            if len(self._pool) > 0:
                client = self._pool.pop()
            else:
                client = self._new_client()

        return client

    def disconnect(self):
        self._cleanup_connections()

    def close(self):
        self._cleanup_connections()

    def _is_ready(self, client):
        if client.blocked:
            return False

        if not client.connected:
            try:
                client.connection.force_connect()
            except Exception:
                if id(client) in self._client_used:
                    self._del_client_used[id(client)]
                return False
        return True

    def _cleanup_connections(self, clients=None):
        with self._lock:
            if self.closed:
                return

            clients = clients or self._pool + list(self._client_used.values())
            try:
                for client in clients:
                    client.disconnect()
                self.closed = True
            except Exception as e:
                logger.warning(
                    "Cleanup clickhouse client connections with error: %s" % e
                )

    def _del_client_used(self, client):
        self._cleanup_connections([client])
        with self._lock:
            del self._client_used[id(client)]

    def _set_client_used(self, client):
        self._client_used[id(client)] = client

    def _new_client(self):
        client = BlockedClient(**self.connection_args)
        self._set_client_used(client)
        return client

    def __del__(self):
        self._cleanup_connections()
