"""
QuantumLane MCP server — the tool/orchestration layer (the main.py analogue).

A thin Model Context Protocol server over the QuantumLane public API. It exposes
TTC transit data as MCP *tools* so an LLM client (Claude, ChatGPT) can answer
questions it otherwise cannot: live vehicle positions, nearby stops, route lookup.

DESIGN: this layer holds NO business logic and talks to NO database. Each tool is
a thin wrapper over api_client (which wraps the public HTTPS API). The valuable
work here is NOT the plumbing — it's the tool DESCRIPTIONS and ARGUMENT SCHEMAS.
FastMCP builds each tool's machine-readable schema from the function signature,
type hints, and docstring; the model reads those to decide which tool to call and
what to pass. So the docstrings ARE the product, not comments.

The recurring failure mode to design against: the user says "the 504" or "the King
car", but the data is keyed on an internal route_id. The route tools accept a
HUMAN route name/number and resolve it internally — the model never has to know
route_id exists.

Transport: streamable HTTP (remote). ChatGPT supports only remote HTTPS MCP
servers (not local stdio), and we want both Claude and ChatGPT to connect, so this
is hosted behind Caddy at https://quantumlane.io/mcp.

Run locally:
    python -m quantumlane_mcp.server     # serves streamable HTTP on :8100
    # then point the MCP Inspector at http://localhost:8100/mcp
"""
from __future__ import annotations

from fastmcp import FastMCP

from quantumlane_mcp import api_client

mcp = FastMCP(
    name="QuantumLane",
    instructions=(
        "Live Toronto (TTC) transit data from the QuantumLane platform. Use these "
        "tools to answer questions about where transit vehicles are right now, which "
        "stops are near a location, and what routes exist. Vehicle positions are live "
        "(at most ~1-2 minutes old). When a user names a route by its number or name "
        "('504', 'the King streetcar', 'Dufferin bus'), pass that string directly to "
        "the route argument — the tools resolve it to the internal route id for you."
    ),
)


@mcp.tool
def vehicles_on_route(route: str) -> dict:
    """Get the live positions of all TTC vehicles currently running a given route.

    Use this to answer "where is the 504 right now", "how many King streetcars are
    out", "where are the Dufferin buses". Returns each vehicle's latitude/longitude,
    heading (bearing in degrees), and speed, as of the last feed poll (~1-2 min old).

    Args:
        route: The route as a rider would name it — its number or name. Examples:
            "504", "King", "the King streetcar", "29", "Dufferin". Do NOT pass an
            internal id; pass what the user said and this tool resolves it.

    Returns a dict with the resolved route, a vehicle count, and the vehicle list.
    If the route can't be matched, returns an error message suggesting list_routes.
    """
    matched = api_client.resolve_route(route)
    if matched is None:
        return {
            "error": (
                f"No TTC route matched '{route}'. Try a route number like '504' or a "
                f"name like 'King'. Call list_routes to see available routes."
            ),
        }

    route_id = matched["route_id"]
    vehicles = api_client.vehicles_for_route_id(route_id)
    return {
        "route": {
            "route_id": route_id,
            "short_name": matched.get("route_short_name"),
            "long_name": matched.get("route_long_name"),
        },
        "vehicle_count": len(vehicles),
        "vehicles": vehicles,
        "note": (
            "Positions are live, at most ~1-2 minutes old. An empty list means no "
            "vehicles are currently reporting on this route (off-hours or no service)."
        ),
    }


@mcp.tool
def nearest_stops(latitude: float, longitude: float, limit: int = 5) -> dict:
    """Find the TTC stops closest to a geographic coordinate, nearest first.

    Use this to answer "what stops are near me", "closest streetcar stop to the CN
    Tower", "TTC stops around 43.65, -79.38". If the user names a landmark rather
    than coordinates, first determine the landmark's lat/long, then call this.

    Args:
        latitude: Decimal degrees, e.g. 43.6426 (Toronto is ~43.6 to 43.8).
        longitude: Decimal degrees, e.g. -79.3871 (Toronto is ~ -79.2 to -79.6;
            note it is NEGATIVE — west of the prime meridian).
        limit: How many stops to return (1-25, default 5).

    Returns each stop's name, coordinates, and distance_m (metres from the query
    point), ordered closest first.
    """
    if not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
        return {
            "error": (
                "latitude must be -90..90 and longitude -180..180 (Toronto longitude "
                "is negative, around -79)."
            )
        }
    limit = max(1, min(limit, 25))

    stops = api_client.stops_near(latitude, longitude, limit)
    return {
        "query_point": {"latitude": latitude, "longitude": longitude},
        "stop_count": len(stops),
        "stops": stops,
    }


@mcp.tool
def list_routes() -> dict:
    """List all TTC routes (number, name, and type).

    Use this when the user asks what routes exist, or when you need to find the right
    route name before calling vehicles_on_route. route_type follows the GTFS
    convention: 0 = tram/streetcar, 1 = subway, 3 = bus.

    Returns the full route catalog (a few hundred routes).
    """
    routes = api_client.list_routes()
    return {"route_count": len(routes), "routes": routes}


def main() -> None:
    """Console-script / module entry point. Serves streamable HTTP for remote clients.

    Binds 0.0.0.0:8100; Caddy terminates TLS and proxies /mcp here.
    """
    mcp.run(transport="http", host="0.0.0.0", port=8100)


if __name__ == "__main__":
    main()