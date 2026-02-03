package interfaces

// -----------------------------------------------------------------------------
// INetworkManager defines the contract for HTTP requests with potential proxy/retry logic.
// -----------------------------------------------------------------------------

type INetworkManager interface {

	// -----------------------------------------------------------------------------

	// Get performs a GET request to the specified URL with parameters.
	// Returns the response body as bytes or an error.
	Get(url string, params map[string]string) ([]byte, error)
}
