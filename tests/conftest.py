"""
pytest configuration and shared fixtures.
"""

import sys
from pathlib import Path

import pytest

# Ensure project root is on path regardless of where pytest is invoked from
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def pytest_configure(config):
    config.addinivalue_line("markers", "db: test requires a live PostgreSQL connection")


@pytest.fixture(scope="session")
def db_available() -> bool:
    """Return True if the test database is reachable."""
    try:
        import psycopg2
        from config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
def skip_if_no_db(db_available):
    """Skip the test if the database is not available."""
    if not db_available:
        import pytest as _pytest
        _pytest.skip("Database not available — start with: docker-compose up -d")
