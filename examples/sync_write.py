"""🔁 **sync_write.py**

Another embedded‑replica example, this time demonstrating that **writes can be
committed locally and then synced afterwards**.  This pattern lets you batch
multiple writes and control exactly when they are pushed upstream.

Before running, set these environment variables to point at your remote
database:

```bash
export LIBSQL_URL="libsql://your-db.turso.io"
export LIBSQL_AUTH_TOKEN="..."
```

The script prints the URL it’s syncing with, creates a table, inserts a row,
commits locally, then explicitly calls ``sync`` to push the changes out.  It
finally reads back and prints the row.
"""

import os
import asyncio
import aiolibsql


async def main():
    print(f"syncing with {os.getenv('LIBSQL_URL')}")
    async with await aiolibsql.connect(
        "hello.db",
        sync_url=os.getenv("LIBSQL_URL"),
        auth_token=os.getenv("LIBSQL_AUTH_TOKEN"),
    ) as conn:
        # create and write as normal
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER);")
        await conn.execute("INSERT INTO users(id) VALUES (1);")
        # local commit persists to the file but does not yet send to remote
        await conn.commit()
        # push local commits to the remote database
        await conn.sync()

        cursor = await conn.execute("SELECT * FROM users")
        print(await cursor.fetchall())


if __name__ == "__main__":
    asyncio.run(main())
