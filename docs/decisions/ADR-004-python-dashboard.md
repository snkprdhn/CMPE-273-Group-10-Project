# ADR-004 — Python dashboard and test tooling

- Status: Accepted, 2026-11-21
- Authority: User-approved Python application code with containerized infrastructure
- Owner: Member_4; load/fault tooling: Member_1

## Context and decision

Replace the planned React/TypeScript/Material UI/Recharts application with **NiceGUI 3.19.0**, authored in Python. Its published package supplies browser assets; project development does not require Node.js, npm, custom JavaScript, or a frontend build chain. [NiceGUI documentation](https://nicegui.io/documentation) describes its Python-first browser UI, charts, tables, and event handling; the [release source requirements](https://raw.githubusercontent.com/zauberzeug/nicegui/v3.19.0/pyproject.toml) permit Python 3.12; the shared dependencies must also satisfy its Starlette and pydantic-core lower bounds (see amendment below).

Keep FastAPI as the separate authoritative REST/SSE boundary. The Python UI server consumes REST snapshots and SSE updates using HTTPX, preserves event IDs/version ordering, and forwards rendered updates through NiceGUI's built-in browser transport. The browser transport is WebSocket/Socket.IO; **the API's SSE contract remains unchanged**. Reconnects reconstruct state from the API, including `snapshot.required`. UI code never reads another service's tables or calculates control policy. Retain accessibility, freshness, authorization, and investigation-evidence acceptance criteria.

NiceGUI holds per-client server state. Use one UI process/replica initially; the larger review profile may exercise two UI replicas only with sticky connections and a reconnect-to-snapshot test. API replica/failover demonstrations remain required and independent of UI replication. Do not claim arbitrary stateless UI scaling.

Use **Locust 2.33.2** in place of k6 so load scenarios are Python. HTTP streaming tests must explicitly measure SSE time-to-first-event, reconnects, cursor continuity, event latency, and disconnects; ordinary request latency alone is insufficient. Use **Playwright's Python API** for browser tests; it packages its driver, so a separately installed Node.js toolchain is unnecessary. Toxiproxy and Testcontainers remain reused infrastructure/testing libraries. [Locust documentation](https://docs.locust.io/en/2.33.2/) and [Playwright Python installation](https://playwright.dev/python/docs/intro) describe these workflows.

## Alternatives and consequences

React would retain the original frontend ecosystem but contradict the requested language boundary. Streamlit would simplify a mostly polling dashboard but require additional work to meet the existing live updates and scoped investigation interactions. NiceGUI provides those building blocks while keeping project code Python. Upstream browser JavaScript still executes in the browser; the requirement concerns authored project code and host tooling.

All changes here are design decisions. No dashboard, SSE bridge, load script, or browser test has been scaffolded or verified.

## Prerequisite recheck amendment — 2026-11-21

Supersede the original NiceGUI 2.24.2 pin with **3.19.0**. Upstream [GHSA-m47w-xwfc-82pf](https://github.com/zauberzeug/nicegui/security/advisories/GHSA-m47w-xwfc-82pf) identifies a chart/grid data-key XSS fixed in 3.19.0; [GHSA-jfrm-rx66-g536](https://github.com/zauberzeug/nicegui/security/advisories/GHSA-jfrm-rx66-g536) identifies file disclosure in older releases.

The patched release requires Starlette >=1.3.1 and pydantic-core >=2.35.0, incompatible with the old FastAPI 0.116.1 / Pydantic 2.11.7 combination. Select FastAPI **0.165.0**, Starlette **1.10.0**, and Pydantic **2.14.4** (core **2.50.5**) together for API/UI environments. Exact release metadata and nine direct constraint comparisons were checked in [recheck evidence](../implementation/PREPARATION_VERIFICATION.md). No environment was installed or fully resolved; runtime/browser and full dependency/security gates remain open. The Python-only authoring and independent service boundaries do not change.
