/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

const (
	caBundleKey = "ca.crt"
	authBearer  = "bearer"
	authBasic   = "basic"
)

// gateConfigError marks a health-gate MISCONFIGURATION (missing/garbled Secret, bad CA, bad
// template) — a config error, not a monitoring outage. It is surfaced as Degraded and MUST NOT
// reach decideGate, so a config mistake can never be laundered into a rollback.
type gateConfigError struct {
	reason string // condition reason, e.g. AuthSecretNotFound
	msg    string
}

func (e *gateConfigError) Error() string { return e.msg }

func configErr(reason, format string, args ...any) *gateConfigError {
	return &gateConfigError{reason: reason, msg: fmt.Sprintf(format, args...)}
}

// gateTransport carries the HTTP client and Authorization header for evaluating a gate.
type gateTransport struct {
	client     *http.Client
	authHeader string // "" when no auth
}

// resolveGateTransport builds the client + auth header for a gate, reading any referenced Secret /
// ConfigMap from the FleetRollout's namespace. Returns a *gateConfigError on any misconfiguration.
func (r *FleetRolloutReconciler) resolveGateTransport(ctx context.Context, ns string, gate *fleetv1alpha1.HealthGate) (gateTransport, error) {
	var tr gateTransport

	if gate.Auth != nil {
		h, err := r.resolveAuthHeader(ctx, ns, gate.Auth)
		if err != nil {
			return tr, err
		}
		tr.authHeader = h
	}

	switch {
	case r.HTTP != nil:
		tr.client = r.HTTP // test injection: use verbatim, ignore TLS block
	case gate.TLS != nil:
		cl, err := r.buildTLSClient(ctx, ns, gate.TLS)
		if err != nil {
			return tr, err
		}
		tr.client = cl
	default:
		tr.client = &http.Client{Timeout: promQLTimeout}
	}
	return tr, nil
}

// resolveAuthHeader reads the auth Secret and returns the Authorization header value.
func (r *FleetRolloutReconciler) resolveAuthHeader(ctx context.Context, ns string, auth *fleetv1alpha1.GateAuth) (string, error) {
	var sec corev1.Secret
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: ns, Name: auth.SecretRef.Name}, &sec); err != nil {
		return "", configErr("AuthSecretNotFound", "auth secret %q not found: %v", auth.SecretRef.Name, err)
	}
	switch auth.Type {
	case authBearer:
		token := string(sec.Data["token"])
		if token == "" {
			return "", configErr("AuthSecretMalformed", "auth secret %q missing non-empty key \"token\"", auth.SecretRef.Name)
		}
		return "Bearer " + token, nil
	case authBasic:
		user, pass := string(sec.Data["username"]), string(sec.Data["password"])
		if user == "" || pass == "" {
			return "", configErr("AuthSecretMalformed", "auth secret %q missing key \"username\" or \"password\"", auth.SecretRef.Name)
		}
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass)), nil
	default:
		return "", configErr("AuthTypeInvalid", "unknown auth type %q", auth.Type)
	}
}

// buildTLSClient constructs an http.Client whose TLS config honors the gate's CA / serverName /
// insecureSkipVerify.
func (r *FleetRolloutReconciler) buildTLSClient(ctx context.Context, ns string, gtls *fleetv1alpha1.GateTLS) (*http.Client, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: gtls.ServerName, InsecureSkipVerify: gtls.InsecureSkipVerify} //nolint:gosec // insecure only if explicitly opted in

	if gtls.CARef != nil {
		pem, err := r.readCABundle(ctx, ns, gtls.CARef)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, configErr("CAInvalid", "CA bundle %q key %q contains no valid certificates", gtls.CARef.Name, caBundleKey)
		}
		cfg.RootCAs = pool
	}

	return &http.Client{Timeout: promQLTimeout, Transport: &http.Transport{TLSClientConfig: cfg}}, nil
}

// readCABundle reads the "ca.crt" key from the referenced Secret or ConfigMap.
func (r *FleetRolloutReconciler) readCABundle(ctx context.Context, ns string, ref *fleetv1alpha1.CASourceReference) ([]byte, error) {
	key := types.NamespacedName{Namespace: ns, Name: ref.Name}
	if ref.Kind == "Secret" {
		var sec corev1.Secret
		if err := r.reader().Get(ctx, key, &sec); err != nil {
			return nil, configErr("CANotFound", "CA secret %q not found: %v", ref.Name, err)
		}
		if b := sec.Data[caBundleKey]; len(b) > 0 {
			return b, nil
		}
		return nil, configErr("CAInvalid", "CA secret %q missing key %q", ref.Name, caBundleKey)
	}
	// default / ConfigMap
	var cm corev1.ConfigMap
	if err := r.reader().Get(ctx, key, &cm); err != nil {
		return nil, configErr("CANotFound", "CA configmap %q not found: %v", ref.Name, err)
	}
	if s := cm.Data[caBundleKey]; s != "" {
		return []byte(s), nil
	}
	if b := cm.BinaryData[caBundleKey]; len(b) > 0 {
		return b, nil
	}
	return nil, configErr("CAInvalid", "CA configmap %q missing key %q", ref.Name, caBundleKey)
}
