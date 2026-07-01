"""
Per-IP rate limiting for the MCP server.

WHY: FastMCP (unlike the FastAPI app, which has slowapi) ships no rate limiting.
Publicizing the endpoint means a looping or abusive client could hammer /mcp ->
the API -> Postgres, and Postgres is shared with live ingestion on a small box.
The risk is resource exhaustion (box falls over), NOT cost — nothing in the MCP's
path is metered. This middleware caps requests per client IP so no single caller
can saturate the box.

Deliberately dependency-free and simple: a fixed-window counter per IP, held in
memory. This is adequate for a low-traffic portfolio demo. It is NOT distributed
(single process, in-memory) and NOT a substitute for a real WAF/edge limiter — if
this ever needed to scale, move the limit to the reverse proxy (Caddy) or an edge.
For one small server, in-memory fixed-window is the right amount of guardrail.
"""
from __future__ import annotations

import time

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RateLimitMiddleware:
    """Fixed-window per-IP rate limiter.

    Allows `limit` requests per `window_seconds` per client IP. Over-limit requests
    get a 429 without reaching the app. Window is fixed (resets every window),
    which is simple and slightly bursty at window edges — fine for this use.
    """

    def __init__(self, app: ASGIApp, limit: int = 60, window_seconds: int = 60) -> None:
        self.app = app
        self.limit = limit
        self.window = window_seconds
        # ip -> (window_start_epoch, count_in_window)
        self._buckets: dict[str, tuple[float, int]] = {}

    def _client_ip(self, request: Request) -> str:
        # Behind Caddy, the real client IP is in X-Forwarded-For (first hop).
        # Fall back to the direct peer if the header is absent.
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _allowed(self, ip: str) -> bool:
        now = time.monotonic()
        start, count = self._buckets.get(ip, (now, 0))
        if now - start >= self.window:
            # Window elapsed: reset.
            self._buckets[ip] = (now, 1)
            return True
        if count < self.limit:
            self._buckets[ip] = (start, count + 1)
            return True
        return False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        ip = self._client_ip(request)

        if not self._allowed(ip):
            response = JSONResponse(
                {"error": "rate_limited", "detail": f"Max {self.limit} requests per {self.window}s."},
                status_code=429,
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)