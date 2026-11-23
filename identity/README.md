# Local Keycloak/OIDC — Member_4

Member_4 owns identity/API/UI/Copilot. Member_1 retains simulation/Registry/
observability, Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium.
This is the independent local identity slice, using Docker Desktop dependencies;
full kind workload/network-policy packaging remains the platform integration gate.
No host JVM, JavaScript, cloud, external model or paid service is needed.

From the repository root, install the updated component hash locks into their
existing independent environments (see Member_4/dashboard runbooks). Start the
existing PostgreSQL dependency using `make infra-up`, then:

```sh
.tools/member4-venv/bin/python -m identity.bootstrap
docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak
curl -fsS http://127.0.0.1:18080/realms/smart-grid/.well-known/openid-configuration
export PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection
.tools/member4-venv/bin/python -m member4 migrate
.tools/member4-venv/bin/python -m member4 replay
.tools/member4-venv/bin/python -m member4 serve
```

In a second terminal:

```sh
.tools/dashboard-venv/bin/python -m dashboard
```

Open [the console](http://127.0.0.1:18086). Usernames are `viewer`, `operator`,
`administrator`. Read their generated passwords locally in
`.tools/identity/local.json`; never paste that file into logs/issues. They are
random synthetic local accounts, with no real email or self-registration.
The independent fixture UI (`make dashboard-demo`) keeps its explicit mock mode.
For legacy configured tokens, **both** API and UI require
`MEMBER4_AUTH_MODE=mock`, plus their documented mock token configuration.
The normal runtime ignores mock token variables. Never expose mock mode publicly.

The issuer is exactly `http://127.0.0.1:18080/realms/smart-grid`, public client
`smart-grid-ui`, API audience `smart-grid-api`. Use 127.0.0.1, not localhost.
The sole callback is `http://127.0.0.1:18086/auth/callback`; no wildcard redirects,
password grant, implicit flow or browser secret. Login uses code + PKCE S256,
state and nonce; access/ID tokens are signature/issuer/audience/expiry checked.
Only realm application roles map to the fixed hierarchy. Actor IDs are immutable
Keycloak subjects; role/actor headers are ignored. The API independently checks
every request. Policy writes require Administrator; scenario and acknowledgement
operations require Operator; reads require Viewer. Successful mutations atomically
retain actor/role/request/idempotency/trace context in `projection_operations`.
No token is stored in the audit. Existing policy/scenario adapters remain contract
mocks; identity does not turn them into live control adapters.

The browser receives an opaque HttpOnly SameSite=Lax cookie; tokens stay in
bounded server memory. The server serializes refresh rotation before API/SSE
requests, rechecks session validity after renewal and validates renewed ID tokens. Refresh failure invalidates the session; open tabs clear their data and show a
signed-out page with **Log in**. Logout invalidates other open tabs as well. SSE closes
at access-token expiry and reconnects with refreshed credentials. **Log out**
invalidates the local session and invokes Keycloak RP-initiated logout. API JWTs
already issued remain valid until their two-minute expiry; no instant revocation
claim. The session lasts at most 30 minutes and ends on UI restart. Multiple UI
workers/replicas and persistent sessions remain a later gate. HTTP cookies omit
Secure only for this fixed loopback development profile; HTTPS deployment needs
Secure cookies and an explicitly reviewed issuer/redirect change.

`identity.auth.authorize_tool` revalidates the bearer and requires Operator for
only the seven frozen read-only tools. It accepts no model-provided roles or
write tool names. Tool execution, entity/evidence scope checks, service identity,
Copilot and provider orchestration remain the next milestone; there is no exposed
tool route or success-returning placeholder.

Kafka, PostgreSQL, Connect and Keycloak administration remain private to the
Docker network/loopback developer ports. No new broker/database/admin ingress or
LAN binding is added. Local users are not a hostile-host security boundary.
The required kind default-deny/Calico allow-deny acceptance is still unverified;
Compose loopback isolation does not prove Kubernetes NetworkPolicy enforcement.
Keycloak uses its own database/login in the existing PostgreSQL primary. Build
context/import/env/password files are ignored and mode 0600 in a 0700 directory.
The local derived image contains synthetic user credentials: never push/export it.

Bootstrap reruns preserve existing credentials and imported realm. To deliberately
recreate **only identity state**, stop Keycloak, reset its database and rebuild:

```sh
docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml stop keycloak
.tools/member4-venv/bin/python -m identity.bootstrap --reset
docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak
```

Reset drops only `local_dev_keycloak` and refuses active connections. It preserves
other service databases/volumes. To rotate all generated local passwords, delete
`.tools/identity/local.json` before the explicit `--reset`; reset also updates
only the Keycloak role password. A normal bootstrap never rotates existing state. Stop with the scoped `stop keycloak` command; do not
use `down -v` or `--remove-orphans` against the shared development project.

Verification commands (live test owns free UI port 18086 and cleans up):

```sh
.tools/member4-venv/bin/python -m pytest -q tests/identity/test_auth.py tests/identity/test_browser.py
MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_oidc.py
IDENTITY_LIVE=1 PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m pytest -q tests/identity/test_live.py
make dashboard-test
make contracts BASE_REF=HEAD
```

Real Chromium acceptance covers all three roles, login/logout, real refresh,
read-only tool role guards, then invokes Member_4's direct API tests using transient
Keycloak-issued tokens and disposable PostgreSQL schemas. Tokens are deleted in
`finally`; test artifacts stay ignored. It requires both component environments,
Chromium and the local Keycloak/PostgreSQL dependencies. The separate token flow
uses PKCE rather than enabling password grants for tests.
