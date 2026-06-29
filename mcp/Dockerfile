# QuantumLane MCP server image.
#
# Mirrors the api/ service's build pattern. The MCP has only two runtime deps, so
# it installs the package (which pulls fastmcp + httpx) and runs the console script.
FROM python:3.12-slim

WORKDIR /app

# Install deps first (layer caching): copy only the project metadata, install, then
# copy source. Matches the api service's dependency-before-source ordering.
COPY pyproject.toml /app/
COPY src /app/src

RUN pip install --no-cache-dir .

EXPOSE 8100

# Streamable HTTP transport on 0.0.0.0:8100; Caddy proxies /mcp here.
CMD ["quantumlane-mcp"]