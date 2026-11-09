"""Is the latency in the server or in the HTTP client? python load/client_check.py http://api:8000"""
import asyncio
import sys
import time
import urllib.request

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"

for _ in range(2):
    t = time.perf_counter()
    for _ in range(200):
        urllib.request.urlopen(BASE + "/healthz").read()
    print("urllib (new conn each)  ms/req", round((time.perf_counter() - t) / 200 * 1000, 2))


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE) as c:
        for _ in range(2):
            t = time.perf_counter()
            for _ in range(200):
                await c.get("/healthz")
            print("httpx keep-alive        ms/req", round((time.perf_counter() - t) / 200 * 1000, 2))
        t = time.perf_counter()
        await asyncio.gather(*(c.get("/healthz") for _ in range(500)))
        print("httpx 500 concurrent     req/s", round(500 / (time.perf_counter() - t)))


asyncio.run(main())
