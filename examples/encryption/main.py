"""🔒 **encryption/main.py**

Illustrates how to open an **AES‑encrypted local database**.  ``aiolibsql``
accepts an ``encryption_key`` parameter to ``connect`` which will enable
on‑disk encryption using the [SQLCipher](https://www.zetetic.net/sqlcipher/)
format supported by libsql.  Only one key may be used per file, so keep it
safe!

For real applications you should read the key from a secure location such as
an environment variable or secret manager.  This example hardcodes the key
solely for clarity.

The rest of the script is identical to the other introductory examples: it
creates a table, inserts a few rows, and prints them.
"""

import asyncio
import aiolibsql


async def main():
    # Typically you'd do something like:
    # encryption_key = os.getenv("ENCRYPTION_KEY")
    # but for demo we hardcode a value.
    encryption_key = "my-safe-encryption-key"

    # Pass ``encryption_key`` to ``connect`` and the resulting file will be
    # encrypted on disk.  Opening it later requires the same key.
    async with await aiolibsql.connect("local.db", encryption_key=encryption_key) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Encrypted DB rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
