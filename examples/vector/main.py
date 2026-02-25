"""📊 **vector/main.py**

Example using the **vector extension** in libsql.  This script creates a
simple ``movies`` table where each row includes a 3‑element float32 vector.
It then builds a vector index and queries for the nearest neighbours to a
hardcoded query vector.

The code is deliberately verbose with comments, showing each step of the
process.  Run ``python examples/vector/main.py`` to execute.
"""

import asyncio
import aiolibsql


async def main():
    # use a persistent database so you can re-run the example without
    # losing the data
    async with await aiolibsql.connect("vector.db") as conn:
        # drop the table if it already exists so the script is idempotent
        await conn.execute("DROP TABLE IF EXISTS movies")

        # create a table with a 3‑element float vector column
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS movies (title TEXT, year INT, embedding F32_BLOB(3))"
        )

        # build a special index on the embedding column for fast vector queries
        await conn.execute("CREATE INDEX IF NOT EXISTS movies_idx ON movies (libsql_vector_idx(embedding))")

        # insert some sample embeddings using the ``vector32`` constructor
        await conn.execute(
            "INSERT INTO movies (title, year, embedding) VALUES "
            "('Napoleon', 2023, vector32('[1,2,3]')), "
            "('Black Hawk Down', 2001, vector32('[10,11,12]')), "
            "('Gladiator', 2000, vector32('[7,8,9]')), "
            "('Blade Runner', 1982, vector32('[4,5,6]'))"
        )

        # query for the three nearest neighbours to the vector [4,5,6]
        cursor = await conn.execute(
            "SELECT title, year "
            "FROM vector_top_k('movies_idx', '[4,5,6]', 3) "
            "JOIN movies ON movies.rowid = id"
        )
        rows = await cursor.fetchall()
        print("Nearest neighbour movies:", rows)


if __name__ == "__main__":
    asyncio.run(main())
