# Hosted overlay

`kustomization.yaml` holds the settings shared by hosted deployments: review identity, three-hour rolling Kafka retention with size caps, and the Investigator evidence retention. The [AKS overlay](../aks/kustomization.yaml) inherits it (AKS, then hosted, then [kind dev](../kind/dev/kustomization.yaml), then [kind base](../kind/base/kustomization.yaml)).

Azure-specific storage, ingress and Cilium policy belong in `infra/aks`. See the [AKS runbook](../aks/AGENT_RUNBOOK.md) for rollout and verification.
