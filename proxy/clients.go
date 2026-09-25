package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// ==========================================
// F5 BIG-IP mock network framework
// ==========================================

// CertUpdateRequest models a Let's Encrypt-driven TLS certificate rotation
// against an F5 BIG-IP virtual server / client-SSL profile.
type CertUpdateRequest struct {
	VirtualServer string `json:"virtual_server"`
	CommonName    string `json:"common_name"`
	CertPEM       string `json:"cert_pem"`
	KeyPEM        string `json:"key_pem"`
	ChainPEM      string `json:"chain_pem"`
	// ACMEOrderID correlates the LE workflow order that produced this cert.
	ACMEOrderID string `json:"acme_order_id"`
}

// CertUpdateResult is the downstream acknowledgement of a cert push.
type CertUpdateResult struct {
	ProfileName   string    `json:"profile_name"`
	AppliedAt     time.Time `json:"applied_at"`
	FingerprintSHA string   `json:"fingerprint_sha"`
	TLSVersion    string    `json:"tls_version"`
}

// RoutingUpdateRequest models an F5 traffic-routing policy mutation.
type RoutingUpdateRequest struct {
	PoolName   string   `json:"pool_name"`
	Members    []string `json:"members"`
	PersistKey string   `json:"persist_key"`
}

// F5BIGIPClient abstracts authenticated control-plane calls to F5 ADSP / BIG-IP.
type F5BIGIPClient interface {
	// Authenticate performs the initial TLS mutual-auth handshake and token exchange.
	Authenticate(ctx context.Context) error
	// UpdateCertificate pushes a Let's Encrypt certificate onto a client-SSL profile.
	UpdateCertificate(ctx context.Context, req CertUpdateRequest) (*CertUpdateResult, error)
	// ModifyRouting updates pool membership / persistence for a virtual server.
	ModifyRouting(ctx context.Context, req RoutingUpdateRequest) error
	// HealthProbe verifies the management API is reachable post-handshake.
	HealthProbe(ctx context.Context) error
}

// MockF5Client simulates F5 BIG-IP API authentication and cert/routing ops
// without contacting a live appliance. Suitable for local saga drills.
type MockF5Client struct {
	mu            sync.Mutex
	authenticated bool
	token         string
	appliedCerts  map[string]CertUpdateResult
	// FailAuth forces Authenticate to return ErrAuthHandshakeFailed.
	FailAuth bool
	// FailCertPush forces UpdateCertificate to return NonRetryableInfraError.
	FailCertPush bool
}

// NewMockF5Client returns a ready-to-use in-memory F5 simulator.
func NewMockF5Client() *MockF5Client {
	return &MockF5Client{
		appliedCerts: make(map[string]CertUpdateResult),
	}
}

func (c *MockF5Client) Authenticate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailAuth {
		return contracts.ErrAuthHandshakeFailed
	}
	// Simulate mTLS handshake negotiation against BIG-IP iControl REST.
	_ = tls.VersionTLS13
	c.token = fmt.Sprintf("f5-mock-token-%d", time.Now().UnixNano())
	c.authenticated = true
	return nil
}

func (c *MockF5Client) requireAuth() error {
	if !c.authenticated || c.token == "" {
		return contracts.ErrAuthHandshakeFailed
	}
	return nil
}

func (c *MockF5Client) UpdateCertificate(ctx context.Context, req CertUpdateRequest) (*CertUpdateResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireAuth(); err != nil {
		return nil, err
	}
	if c.FailCertPush {
		return nil, contracts.NonRetryableInfraError
	}
	if req.CommonName == "" || req.CertPEM == "" || req.KeyPEM == "" {
		return nil, fmt.Errorf("%w: incomplete Let's Encrypt certificate bundle", contracts.NonRetryableInfraError)
	}
	result := CertUpdateResult{
		ProfileName:    fmt.Sprintf("clientssl-%s", req.VirtualServer),
		AppliedAt:      time.Now().UTC(),
		FingerprintSHA: fmt.Sprintf("sha256:mock-%s-%s", req.CommonName, req.ACMEOrderID),
		TLSVersion:     "TLS1.3",
	}
	c.appliedCerts[req.VirtualServer] = result
	return &result, nil
}

func (c *MockF5Client) ModifyRouting(ctx context.Context, req RoutingUpdateRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireAuth(); err != nil {
		return err
	}
	if req.PoolName == "" {
		return fmt.Errorf("%w: empty pool name", contracts.NonRetryableInfraError)
	}
	return nil
}

func (c *MockF5Client) HealthProbe(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requireAuth()
}

// ==========================================
// Zscaler Zero-Trust mock network framework
// ==========================================

// ZscalerPolicyAction enumerates Zero-Trust policy mutations.
type ZscalerPolicyAction string

const (
	ZscalerActionAllow  ZscalerPolicyAction = "ALLOW"
	ZscalerActionBlock  ZscalerPolicyAction = "BLOCK"
	ZscalerActionDelete ZscalerPolicyAction = "DELETE"
)

// ZscalerPolicyRequest models a Zero-Trust access-policy mutation payload.
type ZscalerPolicyRequest struct {
	PolicyID     string              `json:"policy_id"`
	Action       ZscalerPolicyAction `json:"action"`
	AppSegment   string              `json:"app_segment"`
	UserGroup    string              `json:"user_group"`
	SourceIPCIDR string              `json:"source_ip_cidr"`
}

// ZscalerPolicyResult acknowledges a successful Zero-Trust policy write.
type ZscalerPolicyResult struct {
	PolicyID  string    `json:"policy_id"`
	Revision  int64     `json:"revision"`
	AppliedAt time.Time `json:"applied_at"`
}

// ZscalerClient abstracts authenticated Zscaler Internet Access / Private Access
// policy control-plane operations.
type ZscalerClient interface {
	// Authenticate exchanges API key + secret for a short-lived session token.
	Authenticate(ctx context.Context) error
	// UpsertPolicy creates or updates a Zero-Trust access policy.
	UpsertPolicy(ctx context.Context, req ZscalerPolicyRequest) (*ZscalerPolicyResult, error)
	// DeletePolicy removes a Zero-Trust policy by ID.
	DeletePolicy(ctx context.Context, policyID string) error
	// GetPolicyRevision returns the current policy revision for optimistic concurrency.
	GetPolicyRevision(ctx context.Context, policyID string) (int64, error)
}

// MockZscalerClient simulates Zscaler Zero-Trust policy APIs in-process.
type MockZscalerClient struct {
	mu            sync.Mutex
	authenticated bool
	sessionToken  string
	policies      map[string]ZscalerPolicyResult
	// FailAuth forces Authenticate to return ErrAuthHandshakeFailed.
	FailAuth bool
}

// NewMockZscalerClient returns a ready-to-use in-memory Zscaler simulator.
func NewMockZscalerClient() *MockZscalerClient {
	return &MockZscalerClient{
		policies: make(map[string]ZscalerPolicyResult),
	}
}

func (c *MockZscalerClient) Authenticate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailAuth {
		return contracts.ErrAuthHandshakeFailed
	}
	c.sessionToken = fmt.Sprintf("zs-session-%d", time.Now().UnixNano())
	c.authenticated = true
	return nil
}

func (c *MockZscalerClient) requireAuth() error {
	if !c.authenticated || c.sessionToken == "" {
		return contracts.ErrAuthHandshakeFailed
	}
	return nil
}

func (c *MockZscalerClient) UpsertPolicy(ctx context.Context, req ZscalerPolicyRequest) (*ZscalerPolicyResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireAuth(); err != nil {
		return nil, err
	}
	if req.PolicyID == "" || req.AppSegment == "" {
		return nil, fmt.Errorf("%w: incomplete Zero-Trust policy payload", contracts.NonRetryableInfraError)
	}
	prev := c.policies[req.PolicyID]
	result := ZscalerPolicyResult{
		PolicyID:  req.PolicyID,
		Revision:  prev.Revision + 1,
		AppliedAt: time.Now().UTC(),
	}
	c.policies[req.PolicyID] = result
	return &result, nil
}

func (c *MockZscalerClient) DeletePolicy(ctx context.Context, policyID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireAuth(); err != nil {
		return err
	}
	if policyID == contracts.RootCoreEdgeID {
		return contracts.ErrRootCoreMutation
	}
	delete(c.policies, policyID)
	return nil
}

func (c *MockZscalerClient) GetPolicyRevision(ctx context.Context, policyID string) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireAuth(); err != nil {
		return 0, err
	}
	p, ok := c.policies[policyID]
	if !ok {
		return 0, fmt.Errorf("policy %q not found", policyID)
	}
	return p.Revision, nil
}
