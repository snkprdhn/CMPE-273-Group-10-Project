# ADR-032 — Member_4 local Keycloak identity and authorization

- Status: Accepted, 2026-11-22
- Authority: Requested local identity milestone and routine implementation choices
- Owners: Member_4 projection/API/UI/identity/Copilot; Member_1 simulation/Registry/
  observability; Member_2 Kafka/PyFlink; Member_3 control/PostgreSQL/Debezium

Use the selected Keycloak 26.1.4 ARM64 image with its immutable manifest digest,
its own database/login in the existing PostgreSQL primary, and generated ignored
local passwords. Bootstrap imports `smart-grid`, the public `smart-grid-ui`
code+PKCE S256 client, a `smart-grid-api` audience mapper, subject/role mappers,
the three inherited application roles and synthetic accounts. No password grant,
implicit flow, wildcard callback, browser secret, registration or external email.
Normal startup preserves the imported realm; the explicit scoped reset recreates
only identity state. Never publish the local import image containing credentials.

Reuse HTTPX for OAuth exchanges and use PyJWT 2.15.1 / cryptography 50.0.2 for
RS256/JWKS verification rather than authoring cryptography. Independent API and
UI environments include the same small hash lock; other member environments are
unchanged. Verify issuer, audience, signature, expiry, issued-at, subject, access
token type and authorized client. Map only signed realm application roles. ID
tokens additionally require UI audience, state/nonce and matching subject. Unknown
keys trigger JWKS refresh using PyJWT's bounded cache; unverifiable keys fail shut.

The NiceGUI Python server is a local BFF. Opaque HttpOnly SameSite cookies identify
bounded in-memory sessions; access/refresh/ID tokens never enter browser storage.
Serialize renewal per session, rotate refresh tokens, and invalidate failed/expired
sessions. Recheck session existence/expiry after awaited renewal so concurrent
logout cannot release a new bearer. Validate renewed ID tokens and reject malformed
provider responses or nonnumeric/nonfinite JWT time claims. Open dashboard tabs
clear their data and navigate to a signed-out page when their session ends. A failed login cannot create a session. Logout requires the per-session
CSRF value, removes the session and redirects through standard RP logout. Disable
access logs containing OAuth callback codes and use no-store/no-referrer pages.
API streams end at token expiry; reconnect reauthorizes. Offline JWT verification
means an already issued API token can remain valid for its two-minute lifetime
after logout. Local sessions end after 30 minutes or a UI restart. No multi-worker
session/failover or production TLS claim follows from this local slice.

## Resolved differences

- ADR-030/031's mock-only identity is superseded for normal runtimes; the explicit
  standalone/fixture mode stays available and tested. Environment bearer variables
  cannot silently select mock mode. Application constructors retain injectable
  mock identities for independent tests.
- The frozen OpenAPI policy route requires **Administrator**, consistent with the
  prose API contract. Operator may run scenarios/acknowledge; Viewer may read.
  Administrator inherits all permissions. No v1 contract shape or minimum role is
  weakened. Successful changes retain the existing transactional audit and exact
  immutable Keycloak subject; actor/role headers never grant authority.
- OpenAPI's `keycloak.local` discovery metadata describes a deployment alias. The
  tested standalone profile pins loopback issuer `127.0.0.1:18080`, with UI port
  18086 and exact callback/logout URLs. No host-file edits or issuer aliases are
  accepted by token verification. The eventual kind gateway needs its own tested
  issuer/redirect profile rather than disabling validation.
- This identity slice provides a reusable, tested tool authorization guard that
  independently revalidates Operator permission and denies every non-allowlisted
  tool. It exposes no tool execution route. Tool Gateway dispatch, scoped evidence,
  service-to-service authentication, budgets and deterministic Copilot execution
  remain the next milestone; a model cannot supply roles to the guard.
- Keep existing Kafka/database/Connect/admin developer ports loopback-only; no
  public ingress is added. This does not implement the later kind/Calico network
  policy acceptance, Kafka ACLs or live Member_1/3 adapters. All infrastructure
  remains free/local on Docker Desktop with built-in Kubernetes disabled.

See [runbook](../../identity/README.md), [status](../implementation/IMPLEMENTATION_STATUS.md).
OIDC endpoints follow [Keycloak documentation](https://www.keycloak.org/securing-apps/oidc-layers);
subject mapping follows [Keycloak's mapper reference](https://www.keycloak.org/admin-api/protocol-mappers).
