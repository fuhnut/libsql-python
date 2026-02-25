"""🔄 **sync/main.py**

Demonstrates the **embedded replica** pattern where a local database file
keeps itself synchronized with a remote Turso/libsql database.

Before running you must set two environment variables:

- ``TURSO_DATABASE_URL`` – the remote database URL (``libsql://...``)
- ``TURSO_AUTH_TOKEN`` – a valid auth token for that database

The script connects to ``local.db`` with ``sync_url`` pointing at the
remote.  It then calls ``conn.sync()`` to pull any remote changes before
making local writes.  After inserting some rows, it prints the table.

Use this example when you want fast local reads with occasional writes that
are propagated to a central database.
"""

import aiolibsql
import os
import asyncio


async def main():
    # read connection info from environment
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # ``sync_url`` enables replica syncing. ``offline`` can be passed to make
    # the replica read-only.
    async with await aiolibsql.connect(
        "local.db", sync_url=url, auth_token=auth_token
    ) as conn:
        # always sync before doing work so you start from the latest state
        await conn.sync()

        # normal SQL operations follow
        await conn.execute("DROP TABLE IF EXISTS users;")
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Replica rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
