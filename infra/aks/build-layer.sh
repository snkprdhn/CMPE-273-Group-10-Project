#!/usr/bin/env bash
# Build a source-only layer for the AKS review cluster, push it to ACR, record its digest.
#   infra/aks/build-layer.sh <fleet|aggregation|action|ui|api|registry> <tag>
# Reads base digests from .tools/aks-demo/bases.env and writes .tools/aks-demo/images.json.
# The rollout itself is separate (tools.aks_demo apply, or kubectl set image) and needs approval.
set -euo pipefail
target="$1"; tag="$2"
cd "$(dirname "$0")/../.."
registry=smartgridevreview9e9.azurecr.io/smart-grid-kind
set -a; . .tools/aks-demo/bases.env; set +a
docker build --platform linux/amd64 --provenance=false -f infra/aks/Dockerfile.layers --target "$target" \
  --build-arg AGGREGATION_BASE --build-arg ACCEPTANCE_BASE --build-arg ACTION_BASE --build-arg UI_BASE --build-arg API_BASE --build-arg REGISTRY_BASE \
  -t "$registry/$target:$tag" . 2>&1 | grep -E "ERROR|error:" || true
test "$(docker image inspect "$registry/$target:$tag" --format '{{.Os}}/{{.Architecture}}')" = linux/amd64
digest=$(docker push "$registry/$target:$tag" 2>&1 | sed -nE 's/.*digest: (sha256:[a-f0-9]{64}).*/\1/p' | tail -n 1)
test -n "$digest"
python3 - "$target" "$registry/$target@$digest" <<'PY'
import json, sys
path = '.tools/aks-demo/images.json'
images = json.load(open(path))
images[sys.argv[1]] = sys.argv[2]
json.dump(images, open(path, 'w'), indent=1)
PY
echo "$target $tag $digest"
