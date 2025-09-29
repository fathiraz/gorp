package security

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"
)

// ConnectionSecurity provides secure connection handling
type ConnectionSecurity struct {
	config      *TLSConfig
	certPool    *x509.CertPool
	certificate *tls.Certificate
}

// SecureConnectionConfig holds secure connection configuration
type SecureConnectionConfig struct {
	TLS                    *TLSConfig
	ConnectionTimeout      time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	KeepAlive             time.Duration
	MaxIdleConns          int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ExpectContinueTimeout time.Duration
	DisableCompression    bool
	ForceAttemptHTTP2     bool
}

// CertificateInfo holds certificate information
type CertificateInfo struct {
	Subject     string
	Issuer      string
	NotBefore   time.Time
	NotAfter    time.Time
	SerialNumber string
	DNSNames    []string
	IPAddresses []net.IP
	IsCA        bool
	KeyUsage    x509.KeyUsage
}

// ConnectionSecurityEvent represents a security event during connection
type ConnectionSecurityEvent struct {
	Type        string
	Severity    SeverityLevel
	Message     string
	Details     map[string]interface{}
	Timestamp   time.Time
	RemoteAddr  string
	LocalAddr   string
}

// NewConnectionSecurity creates a new connection security manager
func NewConnectionSecurity(config *TLSConfig) *ConnectionSecurity {
	if config == nil {
		config = DefaultTLSConfig()
	}

	cs := &ConnectionSecurity{
		config: config,
	}

	if err := cs.initialize(); err != nil {
		// Log error but don't fail completely
		fmt.Printf("Warning: Failed to initialize connection security: %v\n", err)
	}

	return cs
}

// initialize sets up TLS configuration and certificates
func (cs *ConnectionSecurity) initialize() error {
	if !cs.config.Enabled {
		return nil
	}

	// Create certificate pool
	cs.certPool = x509.NewCertPool()

	// Load CA certificate if provided
	if cs.config.CAFile != "" {
		caCert, err := ioutil.ReadFile(cs.config.CAFile)
		if err != nil {
			return fmt.Errorf("failed to read CA certificate: %w", err)
		}

		if !cs.certPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to parse CA certificate")
		}
	} else {
		// Use system root CAs
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			return fmt.Errorf("failed to load system root CAs: %w", err)
		}
		cs.certPool = systemRoots
	}

	// Load client certificate if provided
	if cs.config.CertFile != "" && cs.config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cs.config.CertFile, cs.config.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load client certificate: %w", err)
		}
		cs.certificate = &cert
	}

	return nil
}

// GetTLSConfig returns a configured tls.Config
func (cs *ConnectionSecurity) GetTLSConfig() *tls.Config {
	if !cs.config.Enabled {
		return nil
	}

	tlsConfig := &tls.Config{
		RootCAs:            cs.certPool,
		InsecureSkipVerify: cs.config.InsecureSkipVerify,
		MinVersion:         cs.config.MinVersion,
		MaxVersion:         cs.config.MaxVersion,
		CipherSuites:       cs.config.CipherSuites,
		ServerName:         cs.config.ServerName,
	}

	if cs.certificate != nil {
		tlsConfig.Certificates = []tls.Certificate{*cs.certificate}
	}

	// Set up verification function for additional checks
	tlsConfig.VerifyPeerCertificate = cs.verifyPeerCertificate

	return tlsConfig
}

// verifyPeerCertificate performs additional certificate verification
func (cs *ConnectionSecurity) verifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if cs.config.InsecureSkipVerify {
		return nil
	}

	// Perform additional security checks
	for _, chain := range verifiedChains {
		for _, cert := range chain {
			// Check certificate expiry
			if time.Now().After(cert.NotAfter) {
				return fmt.Errorf("certificate has expired: %s", cert.Subject)
			}

			// Check if certificate will expire soon (within 30 days)
			if time.Until(cert.NotAfter) < 30*24*time.Hour {
				// Log warning about upcoming expiry
				fmt.Printf("Warning: Certificate will expire soon: %s (expires: %s)\n",
					cert.Subject, cert.NotAfter.Format("2006-01-02"))
			}

			// Check for weak signature algorithms
			if err := cs.checkSignatureAlgorithm(cert); err != nil {
				return err
			}

			// Check key strength
			if err := cs.checkKeyStrength(cert); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkSignatureAlgorithm validates certificate signature algorithm strength
func (cs *ConnectionSecurity) checkSignatureAlgorithm(cert *x509.Certificate) error {
	weakAlgorithms := map[x509.SignatureAlgorithm]bool{
		x509.MD2WithRSA:    true,
		x509.MD5WithRSA:    true,
		x509.SHA1WithRSA:   true,
		x509.DSAWithSHA1:   true,
		x509.ECDSAWithSHA1: true,
	}

	if weakAlgorithms[cert.SignatureAlgorithm] {
		return fmt.Errorf("weak signature algorithm detected: %s", cert.SignatureAlgorithm)
	}

	return nil
}

// checkKeyStrength validates certificate key strength
func (cs *ConnectionSecurity) checkKeyStrength(cert *x509.Certificate) error {
	switch pubKey := cert.PublicKey.(type) {
	case *rsa.PublicKey:
		if pubKey.N.BitLen() < 2048 {
			return fmt.Errorf("RSA key too weak: %d bits (minimum 2048)", pubKey.N.BitLen())
		}
	case *ecdsa.PublicKey:
		if pubKey.Curve.Params().BitSize < 256 {
			return fmt.Errorf("ECDSA key too weak: %d bits (minimum 256)",
				pubKey.Curve.Params().BitSize)
		}
	}

	return nil
}

// GetCertificateInfo extracts certificate information for logging/monitoring
func (cs *ConnectionSecurity) GetCertificateInfo(cert *x509.Certificate) *CertificateInfo {
	return &CertificateInfo{
		Subject:      cert.Subject.String(),
		Issuer:       cert.Issuer.String(),
		NotBefore:    cert.NotBefore,
		NotAfter:     cert.NotAfter,
		SerialNumber: cert.SerialNumber.String(),
		DNSNames:     cert.DNSNames,
		IPAddresses:  cert.IPAddresses,
		IsCA:         cert.IsCA,
		KeyUsage:     cert.KeyUsage,
	}
}

// ValidateConnection performs security validation on a connection
func (cs *ConnectionSecurity) ValidateConnection(conn net.Conn) []ConnectionSecurityEvent {
	var events []ConnectionSecurityEvent

	// Check if connection is encrypted
	if tlsConn, ok := conn.(*tls.Conn); ok {
		events = append(events, cs.validateTLSConnection(tlsConn)...)
	} else if cs.config.Enabled {
		events = append(events, ConnectionSecurityEvent{
			Type:      "unencrypted_connection",
			Severity:  HighSeverity,
			Message:   "Connection is not encrypted but TLS is enabled",
			Timestamp: time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	// Check connection source
	events = append(events, cs.validateConnectionSource(conn)...)

	return events
}

// validateTLSConnection validates TLS-specific aspects
func (cs *ConnectionSecurity) validateTLSConnection(conn *tls.Conn) []ConnectionSecurityEvent {
	var events []ConnectionSecurityEvent

	state := conn.ConnectionState()

	// Check TLS version
	minVersion := cs.config.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12 // Default minimum
	}

	if state.Version < minVersion {
		events = append(events, ConnectionSecurityEvent{
			Type:     "weak_tls_version",
			Severity: HighSeverity,
			Message:  fmt.Sprintf("TLS version %x is below minimum %x", state.Version, minVersion),
			Details: map[string]interface{}{
				"actual_version":  state.Version,
				"minimum_version": minVersion,
			},
			Timestamp:  time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	// Check cipher suite strength
	if err := cs.validateCipherSuite(state.CipherSuite); err != nil {
		events = append(events, ConnectionSecurityEvent{
			Type:     "weak_cipher_suite",
			Severity: MediumSeverity,
			Message:  err.Error(),
			Details: map[string]interface{}{
				"cipher_suite": state.CipherSuite,
			},
			Timestamp:  time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	// Check perfect forward secrecy
	if !cs.hasPerfectForwardSecrecy(state.CipherSuite) {
		events = append(events, ConnectionSecurityEvent{
			Type:     "no_perfect_forward_secrecy",
			Severity: LowSeverity,
			Message:  "Connection does not provide perfect forward secrecy",
			Details: map[string]interface{}{
				"cipher_suite": state.CipherSuite,
			},
			Timestamp:  time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	return events
}

// validateConnectionSource validates the connection source
func (cs *ConnectionSecurity) validateConnectionSource(conn net.Conn) []ConnectionSecurityEvent {
	var events []ConnectionSecurityEvent

	remoteAddr := conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return events
	}

	// Check for localhost connections (might be suspicious in production)
	if host == "127.0.0.1" || host == "::1" || host == "localhost" {
		events = append(events, ConnectionSecurityEvent{
			Type:     "localhost_connection",
			Severity: InfoSeverity,
			Message:  "Connection from localhost detected",
			Details: map[string]interface{}{
				"source_host": host,
			},
			Timestamp:  time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	// Check for private IP ranges in production
	ip := net.ParseIP(host)
	if ip != nil && cs.isPrivateIP(ip) {
		events = append(events, ConnectionSecurityEvent{
			Type:     "private_ip_connection",
			Severity: InfoSeverity,
			Message:  "Connection from private IP address",
			Details: map[string]interface{}{
				"source_ip": ip.String(),
			},
			Timestamp:  time.Now(),
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
		})
	}

	return events
}

// validateCipherSuite checks if cipher suite is secure
func (cs *ConnectionSecurity) validateCipherSuite(cipherSuite uint16) error {
	// List of weak/deprecated cipher suites
	weakCipherSuites := map[uint16]string{
		tls.TLS_RSA_WITH_RC4_128_SHA:                "RC4 is deprecated",
		tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:           "3DES is weak",
		tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:          "RC4 is deprecated",
		tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:        "RC4 is deprecated",
		tls.TLS_RSA_WITH_AES_128_CBC_SHA256:         "CBC mode with SHA256 is vulnerable",
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:   "CBC mode with SHA256 is vulnerable",
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256: "CBC mode with SHA256 is vulnerable",
	}

	if reason, exists := weakCipherSuites[cipherSuite]; exists {
		return fmt.Errorf("weak cipher suite 0x%04x: %s", cipherSuite, reason)
	}

	return nil
}

// hasPerfectForwardSecrecy checks if cipher suite provides PFS
func (cs *ConnectionSecurity) hasPerfectForwardSecrecy(cipherSuite uint16) bool {
	pfsRequired := map[uint16]bool{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: true,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:   true,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: true,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:   true,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:  true,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:    true,
	}

	return pfsRequired[cipherSuite]
}

// isPrivateIP checks if IP is in private range
func (cs *ConnectionSecurity) isPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7", // IPv6 private
	}

	for _, rangeStr := range privateRanges {
		_, network, err := net.ParseCIDR(rangeStr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}

	return false
}

// CreateSecureDialer creates a secure network dialer
func (cs *ConnectionSecurity) CreateSecureDialer(config *SecureConnectionConfig) *net.Dialer {
	if config == nil {
		config = DefaultSecureConnectionConfig()
	}

	return &net.Dialer{
		Timeout:   config.ConnectionTimeout,
		KeepAlive: config.KeepAlive,
		DualStack: true, // Enable IPv4 and IPv6
	}
}

// DefaultSecureConnectionConfig returns default secure connection configuration
func DefaultSecureConnectionConfig() *SecureConnectionConfig {
	return &SecureConnectionConfig{
		TLS:                    DefaultTLSConfig(),
		ConnectionTimeout:      30 * time.Second,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		KeepAlive:             30 * time.Second,
		MaxIdleConns:          100,
		MaxConnsPerHost:       10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
		ForceAttemptHTTP2:     true,
	}
}

// GetRecommendedTLSConfig returns security-hardened TLS configuration
func GetRecommendedTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:             true,
		InsecureSkipVerify:  false,
		MinVersion:          tls.VersionTLS12,
		MaxVersion:          tls.VersionTLS13,
		PreferServerCiphers: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}
}