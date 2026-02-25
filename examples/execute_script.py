"""execute sql script from a file and show basic error handling.

run with ``python examples/execute_script.py``
"""

import os
import asyncio
import aiolibsql


async def execute_script(conn: aiolibsql.Connection, file_path: os.PathLike):
    """Execute every statement in ``file_path`` on ``conn``.

    Parameters
    ----------
    conn
        An open :class:`aiolibsql.Connection` obtained via
        ``await aiolibsql.connect(...)``.
    file_path
        Path to a UTF-8 encoded text file containing one or more SQL
        statements separated by ``;``.  ``executescript`` will execute the
        whole blob of text exactly as if you pasted it into the SQLite CLI.

    The function **always commits** after the script runs successfully.  If an
    error is raised by ``executescript`` it will propagate and the caller is
    responsible for handling the rollback (see :func:`main_with_error`).
    """

    # open and read synchronously; file is small. use aiofiles for huge scripts.
    with open(file_path, "r", encoding="utf-8") as file:
        script = file.read()

    # executescript runs all sql statements in one call, like sqlite3's method.
    await conn.executescript(script)
    # commit since executescript does not.
    await conn.commit()


async def main():
    """Run the normal happy‑path example using an in‑memory database.

    The file ``examples/statements.sql`` contains the following SQL::

        CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        INSERT INTO users (name) VALUES ("Alice"), ("Bob");

    This function:

    1. connects to ``:memory:`` (a fresh database each run),
    2. executes the script, and
    3. selects and prints the contents of ``users``.
    """

    # async with closes connection on exit.
    async with await aiolibsql.connect(":memory:") as conn:
        script_path = os.path.join(os.path.dirname(__file__), "statements.sql")
        # run the script from file.
        await execute_script(conn, script_path)

        # check results.
        cursor = await conn.cursor()
        await cursor.execute("SELECT id, name FROM users ORDER BY id")
        rows = await cursor.fetchall()

        print("\nData in the 'users' table after script execution:")
        for row in rows:
            # row is a tuple; index or unpack it.
            print(f"id={row[0]} name={row[1]}")


async def main_with_error():
    """run a bad script to show rollback.

    writes fail and we roll back, then query to confirm nothing stuck.
    """

    bad_script = os.path.join(os.path.dirname(__file__), "bad_statements.sql")
    # make a bad sql file
    with open(bad_script, "w", encoding="utf-8") as f:
        f.write(
            """
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT);
            -- the next line has a typo (INSRT instead of INSERT)
            INSRT INTO items (value) VALUES ('oops');
            """
        )

    async with await aiolibsql.connect(":memory:") as conn:
        try:
            await execute_script(conn, bad_script)
        except Exception as exc:  # pragma: no cover - demonstration only
            print("caught error executing bad script:", exc)
            # conn is still in a transaction; roll back.
            await conn.rollback()

        # check table absence
        cursor = await conn.cursor()
        await cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='items'"
        )
        exists = await cursor.fetchone()
        print("\nTable 'items' exists after failed script?", bool(exists))


if __name__ == "__main__":
    # run both examples in order: normal then error.
    asyncio.run(main())
    asyncio.run(main_with_error())
