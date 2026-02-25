"""local file example: open or create local.db, run a few statements, print users.
"""

import asyncio
import aiolibsql


async def main():
    # async with closes connection on exit.
    async with await aiolibsql.connect("local.db") as conn:
        # create table if it doesn't exist.
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")

        # insert a few rows.
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        # select and fetch all rows.
        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()

        print("rows in local.db:")
        for row in rows:
            print(row)


if __name__ == "__main__":
    asyncio.run(main())
