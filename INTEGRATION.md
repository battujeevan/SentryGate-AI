# Downstream integration notes

The open demo ships **mock** F5 BIG-IP and Zscaler clients (`proxy/clients.go`) so the control-plane firewall can be exercised without vendor credentials.

## Interfaces

```go
type F5BIGIPClient interface {
    Authenticate(ctx context.Context) error
    UpdateCertificate(ctx context.Context, req CertUpdateRequest) (*CertUpdateResult, error)
    ModifyRouting(ctx context.Context, req RoutingUpdateRequest) error
    HealthProbe(ctx context.Context) error
}

type ZscalerClient interface {
    Authenticate(ctx context.Context) error
    UpsertPolicy(ctx context.Context, req ZscalerPolicyRequest) (*ZscalerPolicyResult, error)
    DeletePolicy(ctx context.Context, policyID string) error
    GetPolicyRevision(ctx context.Context, policyID string) (int64, error)
}
```

## Live mode (future / private)

1. Implement `LiveF5Client` against iControl REST with mTLS.
2. Implement `LiveZscalerClient` against ZIA/ZPA APIs with short-lived tokens.
3. Wire implementations in `cmd/worker` activity layer (not in the ingress validator).
4. Keep mocks as the default for CI and public demos.

## Activity wiring guidance

Infrastructure side effects belong in Temporal **activities**, after the proxy has accepted the proposal. The proxy remains a pure policy + dispatch edge so compliance rules stay auditable without redeploying vendor SDK code.
