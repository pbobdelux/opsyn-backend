"""
Standalone Base class for SQLAlchemy ORM models.

Defined here — separate from database.py — so that models can import Base
without triggering database.py's engine creation at module level.  This
breaks the circular import chain that previously caused bootstrap schema
recovery to never execute:

    main.py → models/__init__.py → database.py (engine created!) → crash

With this module, the import chain becomes:

    main.py → models/__init__.py → models.base (no engine) → safe

database.py imports Base from here as well, so there is a single source of
truth for the declarative base.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
