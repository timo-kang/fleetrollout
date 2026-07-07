# FleetRollout

A Kubernetes operator for **progressive (wave-by-wave) rollouts to edge/fleet nodes**, with **health-gated promotion** and **automatic rollback**.

> ⚠️ Early stage / work in progress. Built as an open, generic exploration of fleet deployment safety — not tied to any product.

## Why

Kubernetes' built-in `Deployment` rollout assumes a datacenter: homogeneous nodes, stable networking, and cheap rollbacks. **Edge and robot/IoT fleets break those assumptions** — intermittent connectivity, heterogeneous hardware, and "if a node dies in the field, a human has to physically go to it." That makes *staged rollout + health gates + automatic rollback* essential, yet there's no thin, standard building block for it.

FleetRollout is a small controller that fills that gap.

## What it does

- **Wave-by-wave rollout** — update the fleet in controlled increments (`waveSize` as a count or percentage) instead of all at once.
- **Health-gated promotion** — advance to the next wave only when an optional PromQL health check passes.
- **Automatic rollback** — on wave failure, roll back per `rollbackPolicy`.
- **Node targeting** — select the fleet by label (`targetSelector`).

## Example

```yaml
apiVersion: fleet.fleetrollout.io/v1alpha1
kind: FleetRollout
metadata:
  name: camera-agent
spec:
  targetSelector:
    matchLabels:
      fleet-group: field-robots
  image: registry.example.com/camera-agent:v2.3.0
  waveSize: "20%"
  rollbackPolicy: OnFailure
  healthGate:
    prometheusURL: http://prometheus.monitoring:9090
    query: 'min(up{job="camera-agent"}) == 1'
    timeoutSeconds: 300
```

```
$ kubectl get fleetrollout
NAME           PHASE         WAVE   UPDATED   AGE
camera-agent   Progressing   2      18        4m
```

## Architecture (early)

A `controller-runtime` reconcile loop that:
1. resolves target nodes from `targetSelector` and partitions them into waves,
2. rolls out `image` to the current wave and waits for readiness,
3. evaluates the optional `healthGate` (PromQL) before promoting,
4. advances or rolls back, updating `status` (`phase`, `currentWave`, `updatedNodes`).

## Roadmap

- [x] `FleetRollout` CRD + scaffold
- [ ] MVP reconcile: wave partitioning + readiness-based promotion
- [ ] PromQL health gate + automatic rollback
- [ ] Offline-node re-join convergence
- [ ] kind-based e2e + Helm chart

## Local development

```sh
make install        # install CRDs into the current cluster (kind)
make run            # run the controller locally
```

Requires Go 1.24+, `kubebuilder`, and `kind` (or any Kubernetes cluster).

## License

Apache-2.0 — see [LICENSE](LICENSE).
