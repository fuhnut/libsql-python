#!/usr/bin/env python3
"""🐍 **SQLAlchemy async example**

This script demonstrates how to use **SQLAlchemy's ORM** with
``aiolibsql`` as the underlying database driver.  The dialect is provided by
``aiolibsql_sqlalchemy``, which must be imported before creating an engine.

### Prerequisites

```bash
pip install sqlalchemy
```

The example defines two related models (``User`` and ``Address``), persists
some objects, and then queries them back using SQLAlchemy's query API.

Run:

```bash
python examples/sqlalchemy/example.py
```

You should see SQLAlchemy emit the generated SQL and print two user objects.

This file is intentionally verbose: every block has comments explaining what
SQLAlchemy is doing so that newcomers can follow along.
"""

import asyncio
import sys
import os

# Add the repo root to sys.path so we can import the local aiolibsql packages
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# installing the dialect registers ``sqlite+aiolibsql://`` as a valid URL
import aiolibsql_sqlalchemy  # registers the dialect

from sqlalchemy import String, ForeignKey, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typing import List, Optional


class Base(DeclarativeBase):
    pass


class User(Base):
    # SQLAlchemy declarative class defining a ``user_account`` table
    __tablename__ = "user_account"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]  # optional additional column
    # one-to-many relationship with ``Address``; ``cascade`` ensures that
    # deleting a User also deletes its addresses
    addresses: Mapped[List["Address"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        # Helpful when printing objects during debugging
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Address(Base):
    # Simple address table linked to ``User`` via foreign key
    __tablename__ = "address"
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    # foreign key column referencing ``user_account.id``
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")

    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"


def main():
    # create a synchronous SQLAlchemy engine; ``echo=True`` logs SQL to stdout
    engine = create_engine("sqlite+aiolibsql://", echo=True)

    # create the tables defined by our ORM models
    Base.metadata.create_all(engine)

    # open a session to add some rows
    with Session(engine) as session:
        spongebob = User(
            name="spongebob",
            fullname="Spongebob Squarepants",
            addresses=[Address(email_address="spongebob@sqlalchemy.org")],
        )
        sandy = User(
            name="sandy",
            fullname="Sandy Cheeks",
            addresses=[
                Address(email_address="sandy@sqlalchemy.org"),
                Address(email_address="sandy@squirrelpower.org"),
            ],
        )
        patrick = User(name="patrick", fullname="Patrick Star")
        # add several objects in one transaction
        session.add_all([spongebob, sandy, patrick])
        session.commit()

    # open a new session to run a query
    with Session(engine) as session:
        stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))
        for user in session.scalars(stmt):
            print(user)


if __name__ == "__main__":
    main()
