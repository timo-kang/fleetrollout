# Releasing FleetRollout

Releases are cut by pushing a semver tag. The [`release.yml`](../.github/workflows/release.yml)
workflow then publishes everything from that tag.

## Cut a release

```sh
git tag v0.2.0
git push origin v0.2.0
```

The workflow builds and publishes, all pinned to the tag:

- **Multi-arch image** — `ghcr.io/timo-kang/fleetrollout:v0.2.0` and `:latest` (linux/amd64, linux/arm64).
- **OCI Helm chart** — `oci://ghcr.io/timo-kang/charts/fleetrollout`, chart version `0.2.0`, deploying the pinned image.
- **GitHub Release** — with `install.yaml` (consolidated CRD + RBAC + manager) and the chart `.tgz` attached, plus auto-generated notes.

Tag names map as: image tag = `v0.2.0` (keeps the `v`); Helm chart/app version = `0.2.0` (semver, no `v`).

## One-time: make the GHCR packages public

GHCR packages are **private** on first push. So users can `helm install` / `kubectl apply`
without auth, make both packages public once, under
`https://github.com/users/timo-kang/packages`:

- `fleetrollout` (the container image)
- `charts/fleetrollout` (the Helm chart)

For each: *Package settings → Danger Zone → Change visibility → Public*. Also link each package
to the repository so it appears on the repo sidebar.

## Versioning / compatibility

- **v1alpha1** — the API may still change between releases. CRD schema changes are applied with
  server-side apply (the CRD embeds a full `PodTemplateSpec`, which exceeds the client-side apply
  annotation limit). On a breaking status/spec change, recreate affected `FleetRollout` objects.
- **Kubernetes 1.27+** required (pod `schedulingGates`); 1.30+ recommended (GA).
- Before upgrading with a rollback in flight, resolve it first (set `spec.image`/`spec.template`
  to the known-good) or delete and recreate the CR — the controller resumes rolling *forward* to
  the current spec on restart.
