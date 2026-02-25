"""🔗 **remote_connect.py**

An explicit, step‑by‑step example showing how to **connect to any remote
libsql/Turso database** using a URL and authentication token stored in the
environment.  This file is similar to ``examples/remote/main.py`` but is
kept at the top level for quick experimentation.

## Usage

```bash
export LIBSQL_URL="libsql://your-db.turso.io"
export LIBSQL_AUTH_TOKEN="..."
python examples/remote_connect.py
```

When this script runs it will:

1. print the target URL for sanity
2. open a connection with ``aiolibsql.connect``
3. create a simple ``users`` table if needed
4. insert a row and commit the transaction
5. issue a ``SELECT`` and print the result

The code includes comments on every step so beginners can follow along.
"""

import os
import asyncio
import aiolibsql


async def main():
    # show the URL we are about to connect to
    print(f"connecting to {os.getenv('LIBSQL_URL')}")

    # ``database`` parameter accepts either a path or a libsql:// URL
    async with await aiolibsql.connect(
        database=os.getenv("LIBSQL_URL"),
        auth_token=os.getenv("LIBSQL_AUTH_TOKEN"),
    ) as conn:
        # create a table if it doesn't already exist
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER);")
        # insert a single row; placeholders and parameters could also be used
        await conn.execute("INSERT INTO users(id) VALUES (10);")
        # changes are not sent remotely until we commit
        await conn.commit()

        # read back the inserted row and print it
        cursor = await conn.execute("SELECT * FROM users")
        print(await cursor.fetchall())


if __name__ == "__main__":
    asyncio.run(main())
