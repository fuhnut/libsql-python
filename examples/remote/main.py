"""🌐 **remote/main.py**

Connect to a **remote Turso/libsql database** and run simple DDL/DML
actions.  This example shows how to supply the connection URL and
authentication token via environment variables and how to ``commit`` the
changes.

Before running set the following environment variables:

```bash
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="eyJhbGciOi..."
```

Then run the script with ``python examples/remote/main.py``.  It will create
or recreate the ``users`` table on the remote database and print the rows.
"""

import aiolibsql
import os
import asyncio


async def main():
    # read connection info from environment variables
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # remote connections require an auth token
    async with await aiolibsql.connect(url, auth_token=auth_token) as conn:
        # perform several operations; these are sent over the network to the
        # remote database.
        await conn.execute("DROP TABLE IF EXISTS users;")
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        # remote writes are buffered until you commit explicitly.
        await conn.commit()

        # read back what we just inserted
        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Remote rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
