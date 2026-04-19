# ADR 005: Plain HTML for the website, no JavaScript framework

**Status:** Accepted
**Date:** 2026-04

## Context

The QuantumLane website has four pages:
- Landing page
- Freshness dashboard (live-polling, updates every 10 seconds)
- Explore page (pre-canned queries with SQL shown alongside)
- Architecture summary

The modern default for anything with live data would be Next.js, SvelteKit, or similar. These come with build systems, package managers, and deployment pipelines.

## Decision

Build the site with **plain HTML, Tailwind CSS via CDN, and one vanilla JavaScript file.** No framework. No build step. No bundler.

## Alternatives considered

### Next.js
The industry default.

*Rejected because:*
- Four pages, updated monthly at most. The build system is pure overhead.
- Server components, hydration, and route caching solve problems we do not have.
- Tools churn: Next.js major versions have historically required non-trivial migration work. A static HTML file written in 2026 will still work in 2036.

### Astro
Compelling middle ground: static output, component authoring, islands of interactivity.

*Rejected because:*
- Adds a Node.js build dependency and a package.json to maintain.
- The "islands" feature is the one we'd use (the freshness widget), and plain JavaScript with `fetch` does the same job in 20 lines.

### SvelteKit
Good DX. Smaller output than Next.js.

*Rejected:* same reasoning as Next.js. Not enough content or complexity to justify the build system.

### Pure server-rendered pages via FastAPI + Jinja
Would eliminate the client-side JavaScript entirely.

*Rejected:* the live-polling freshness page needs client-side behavior. A WebSocket would be over-engineered for a 10-second poll. Plain JavaScript is the simplest thing that works.

## Consequences

**Accepted costs:**
- No component reuse beyond what HTML includes offer (none, without templating). The header and footer are duplicated across pages. At four pages, this is a 12-line cost, not a scaling problem.
- No TypeScript. Vanilla JavaScript is fine for ~180 lines of code.
- Tailwind CDN adds a runtime dependency on a third-party CDN. Acceptable for a non-critical project; worst case, the site still functions without styling if the CDN is down.

**Benefits:**
- First paint under 100 milliseconds on any connection. No bundle to download, parse, and execute.
- Zero build step. `git commit` and `rsync` is the entire deployment pipeline for the site.
- Anyone — including non-JavaScript engineers — can read and modify the site.
- Long-term maintenance approaches zero. No dependencies to upgrade.

**Watch for:**
- If the site grows to 10+ pages or needs shared layout logic, revisit with a small static generator (Eleventy, Hugo) before reaching for a framework.
- If interactivity grows to the point where manual DOM updates become error-prone, introduce Alpine.js or HTMX before a full framework.
