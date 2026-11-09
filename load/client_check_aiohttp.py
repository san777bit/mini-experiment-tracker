"""Same measurement with aiohttp, to tell a client problem from a server problem."""
import asyncio
import sys
import time

import aiohttp

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


async def main() -> None:
    print("aiohttp", aiohttp.__version__)
    async with aiohttp.ClientSession(BASE) as s:
        for _ in range(2):
            t = time.perf_counter()
            for _ in range(200):
                async with s.get("/healthz") as r:
                    await r.read()
            print("aiohttp keep-alive      ms/req", round((time.perf_counter() - t) / 200 * 1000, 2))

        async def one() -> None:
            async with s.get("/healthz") as r:
                await r.read()

        t = time.perf_counter()
        await asyncio.gather(*(one() for _ in range(2000)))
        print("aiohttp 2000 concurrent  req/s", round(2000 / (time.perf_counter() - t)))


asyncio.run(main())
