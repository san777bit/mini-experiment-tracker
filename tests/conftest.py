from __future__ import annotations

import sys
from pathlib import Path

import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent))

from helpers import Toxi  # noqa: E402


@pytest_asyncio.fixture
async def toxi():
    t = Toxi()
    await t.reset()
    yield t
    await t.reset()   # never leave a toxic behind for the next test
    await t.close()
