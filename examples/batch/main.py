"""🗂️ **batch/main.py**

Shows how to execute a **batch of SQL statements** in one go using
``executescript``.  This is useful when you need to create tables and seed
initial data in one shot, such as during application startup.

The script uses a temporary ``Cursor`` object because ``Cursor.executescript``
exists in addition to ``Connection.executescript``; both behave identically.

Run this file directly; it will drop/recreate the ``users`` table every
run, then print the rows.
"""

import asyncio
import aiolibsql


async def main():
    # open or create the same local.db file used elsewhere in the examples
    async with await aiolibsql.connect("local.db") as conn:
        # ``cursor()`` returns an independent cursor (synchronous call)
        cur = await conn.cursor()

        # ``executescript`` takes a string containing multiple SQL statements
        # separated by semicolons.  It's equivalent to pasting the text into
        # the sqlite3 CLI.  Useful for schema initialization.
        await cur.executescript(
            """
                DROP TABLE IF EXISTS users;
                CREATE TABLE users (id INTEGER, name TEXT);
                INSERT INTO users VALUES (1, 'first@example.org');
                INSERT INTO users VALUES (2, 'second@example.org');
                INSERT INTO users VALUES (3, 'third@example.org');
            """
        )

        # After the script completes we can run queries like normal.
        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Batch rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
