"""⏳ **transaction/main.py**

Illustrates manual transaction control using ``rollback`` and ``commit``.
The example performs a few operations, then rolls the transaction back so that
those changes are discarded, and finally executes another statement to show
that only the later work persisted.

This is useful when you need to abort a sequence of operations due to an
error or business rule.
"""

import asyncio
import aiolibsql


async def main():
    async with await aiolibsql.connect("local.db") as conn:
        # drop/recreate the table so the script is repeatable
        await conn.execute("DROP TABLE IF EXISTS users")
        await conn.execute("CREATE TABLE users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")

        # rollback discards the two INSERTs above
        await conn.rollback()

        # this insert happens in a new transaction
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Transaction example rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
