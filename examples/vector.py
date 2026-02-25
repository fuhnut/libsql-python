"""📐 **vector.py**

This file is another variation of the vector example that uses the generic
``vector()`` constructor (not ``vector32``) and demonstrates embedding
syntax with a multiline SQL string.  It is essentially the same workflow
as ``examples/vector/main.py`` but kept at the top level for easy access.

Detailed comments are provided below.
"""

import asyncio
import aiolibsql

async def main():
    # connect to a persistent file so the data survives restarts
    async with await aiolibsql.connect("vector.db") as conn:
        # ensure table is fresh each run
        await conn.execute("DROP TABLE IF EXISTS movies")
        await conn.execute("CREATE TABLE movies (title TEXT, year INT, embedding F32_BLOB(3))")
        # vector indexes accelerate nearest‑neighbour queries
        await conn.execute("CREATE INDEX movies_idx ON movies (libsql_vector_idx(embedding))")

        await conn.execute("""
          INSERT INTO movies (title, year, embedding) VALUES
            ('Napoleon', 2023, vector('[1,2,3]')),
            ('Black Hawk Down', 2001, vector('[10,11,12]')),
            ('Gladiator', 2000, vector('[7,8,9]')),
            ('Blade Runner', 1982, vector('[4,5,6]'));
        """)

        cur = await conn.execute("""
          SELECT title, year
          FROM vector_top_k('movies_idx', '[4,5,6]', 3)
          JOIN movies ON movies.rowid = id
        """)

        print(await cur.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
