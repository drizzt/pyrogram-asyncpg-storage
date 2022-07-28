#  pyrogram-asyncpg-storage - PostgreSQL storage for Pyrogram
#
#  Most of the code ripped by:
#
#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import inspect
import time
from typing import List, Tuple, Any

from asyncpg.pool import Pool
from pyrogram.storage import Storage
from pyrogram.storage.sqlite_storage import get_input_peer


__version__ = "0.1"

SCHEMA = """
CREATE TABLE "{schema}"."{namespace}:sessions"
(
    dc_id     INTEGER PRIMARY KEY,
    api_id    INTEGER,
    test_mode INTEGER,
    auth_key  BYTEA,
    date      INTEGER NOT NULL,
    user_id   BIGINT,
    is_bot    BOOL
);

CREATE TABLE "{schema}"."{namespace}:peers"
(
    id             BIGINT PRIMARY KEY,
    access_hash    BIGINT,
    type           TEXT NOT NULL,
    username       TEXT,
    phone_number   TEXT,
    last_update_on BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

CREATE TABLE "{schema}"."{namespace}:version"
(
    number INTEGER PRIMARY KEY
);

CREATE INDEX "idx_{namespace}:peers_id" ON "{schema}"."{namespace}:peers"(id);
CREATE INDEX "idx_{namespace}:peers_username" ON "{schema}"."{namespace}:peers"(username);
CREATE INDEX "idx_{namespace}:peers_phone_number" ON "{schema}"."{namespace}:peers"(phone_number);
"""


class PostgreSQLStorage(Storage):
    VERSION = 1
    USERNAME_TTL = 8 * 60 * 60

    def __init__(self, name: str, pool: Pool, schema: str = "pyrogram"):
        super().__init__(name)

        self._schema = schema
        self._namespace = name
        self.pool = pool
        self.schema = self._schema.replace('"', '""')
        self.namespace = self._namespace.replace('"', '""')
        self.lock = asyncio.Lock()

    async def create(self):
        async with self.lock, self.pool.acquire() as con:
            await con.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
            await con.execute(
                SCHEMA.format(schema=self.schema, namespace=self.namespace)
            )

            await con.execute(
                f'INSERT INTO "{self.schema}"."{self.namespace}:version" VALUES ($1)',
                self.VERSION,
            )

            await con.execute(
                f"""INSERT INTO "{self.schema}"."{self.namespace}:sessions"
                    VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                2,
                None,
                None,
                None,
                0,
                None,
                None,
            )

    async def update(self):
        pass

    async def open(self):
        async with self.pool.acquire() as con:
            if await con.fetchval(
                """SELECT 1 FROM information_schema.tables
                   WHERE table_schema = $1 AND table_name = $2""",
                self._schema,
                f"{self._namespace}:sessions",
            ):
                await self.update()
            else:
                await self.create()

    async def save(self):
        await self.date(int(time.time()))

    async def close(self):
        pass
        #await self.pool.close()

    async def delete(self):
        async with self.pool.acquire() as con:
            await con.execute(f'DROP TABLE "{self.schema}"."{self.namespace}:sessions"')
            await con.execute(f'DROP TABLE "{self.schema}"."{self.namespace}:peers"')
            await con.execute(f'DROP TABLE "{self.schema}"."{self.namespace}:version"')

    async def update_peers(self, peers: List[Tuple[int, int, str, str, str]]):
        async with self.lock, self.pool.acquire() as con:
            await con.executemany(
                f"""INSERT INTO "{self.schema}"."{self.namespace}:peers"
                    (id, access_hash, type, username, phone_number)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT(id) DO UPDATE SET
                    id = EXCLUDED.id, access_hash = EXCLUDED.access_hash,
                    type = EXCLUDED.type, username = EXCLUDED.username,
                    phone_number = EXCLUDED.phone_number,
                    last_update_on = EXTRACT(EPOCH FROM NOW())""",
                peers,
            )

    async def get_peer_by_id(self, peer_id: int):
        if not isinstance(peer_id, int):
            raise KeyError(f"ID not int: {peer_id}")

        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                f"""SELECT id, access_hash, type
                    FROM "{self.schema}"."{self.namespace}:peers"
                    WHERE id = $1""",
                peer_id,
            )

        if r is None:
            raise KeyError(f"ID not found: {peer_id}")

        return get_input_peer(*r)

    async def get_peer_by_username(self, username: str):
        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                f"""SELECT id, access_hash, type, last_update_on
                    FROM "{self.schema}"."{self.namespace}:peers"
                    WHERE username = $1""",
                username,
            )

        if r is None:
            raise KeyError(f"Username not found: {username}")

        if abs(time.time() - r[3]) > self.USERNAME_TTL:
            raise KeyError(f"Username expired: {username}")

        return get_input_peer(*r[:3])

    async def get_peer_by_phone_number(self, phone_number: str):
        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                f"""SELECT id, access_hash, type
                    FROM "{self.schema}"."{self.namespace}:peers"
                    WHERE phone_number = $1""",
                phone_number,
            )

        if r is None:
            raise KeyError(f"Phone number not found: {phone_number}")

        return get_input_peer(*r)

    async def _get(self):
        attr = inspect.stack()[2].function

        async with self.pool.acquire() as con:
            return await con.fetchval(
                f'SELECT {attr} FROM "{self.schema}"."{self.namespace}:sessions"'
            )

    async def _set(self, value: Any):
        attr = inspect.stack()[2].function

        async with self.lock, self.pool.acquire() as con:
            await con.execute(
                f'UPDATE "{self.schema}"."{self.namespace}:sessions" SET {attr} = $1',
                value,
            )

    async def _accessor(self, value: Any = object):
        return await self._get() if value == object else await self._set(value)

    async def dc_id(self, value: int = object):
        return await self._accessor(value)

    async def api_id(self, value: int = object):
        return await self._accessor(value)

    async def test_mode(self, value: bool = object):
        return await self._accessor(value)

    async def auth_key(self, value: bytes = object):
        return await self._accessor(value)

    async def date(self, value: int = object):
        return await self._accessor(value)

    async def user_id(self, value: int = object):
        return await self._accessor(value)

    async def is_bot(self, value: bool = object):
        return await self._accessor(value)

    async def version(self, value: int = object):
        if value == object:
            async with self.pool.acquire() as con:
                return await con.fetchval(
                    f'SELECT number FROM "{self.schema}"."{self.namespace}:version"'
                )
        else:
            async with self.lock, self.pool.acquire() as con:
                await con.execute(
                    f"""UPDATE "{self.schema}"."{self.namespace}:version"
                        SET number = $1""",
                    value,
                )
