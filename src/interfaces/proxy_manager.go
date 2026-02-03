package interfaces

// -----------------------------------------------------------------------------
// IProxyManager defines the contract for managing and rotating proxies.
// -----------------------------------------------------------------------------

type IProxyManager interface {

	// -----------------------------------------------------------------------------

	// GetCurrentProxy returns the currently selected proxy URL (or empty if none).
	GetCurrentProxy() (string, error)

	// -----------------------------------------------------------------------------

	// RotateProxy switches to the next available proxy.
	RotateProxy()

	// -----------------------------------------------------------------------------

	// HasProxies returns true if there are proxies configured.
	HasProxies() bool

	// -----------------------------------------------------------------------------

	// GetUserAgent returns a random User-Agent string.
	GetUserAgent() string

	// -----------------------------------------------------------------------------

	// RefreshProxies scrapes new proxies from external sources.
	// Returns the number of new proxies found or an error.
	RefreshProxies() (int, error)
}
