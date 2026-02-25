"""🧠 **memory/main.py**

Simple demo of using an **in-memory (temporary) database**.  A database
opened with the special name ":memory:" lives only in RAM and is destroyed
as soon as the connection closes.  This is great for tests or scratch work
where you don't want any persistence.

Run the script multiple times and you'll see the table is always empty at
startup, because nothing is saved between runs.
"""

import asyncio
import aiolibsql


async def main():
    # ``:memory:`` creates a fresh database for this connection only.
    async with await aiolibsql.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("In-memory rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
