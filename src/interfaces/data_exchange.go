package interfaces

// -----------------------------------------------------------------------------
// IDataExchanger defining the interface for sharing data with external systems (Server/Push).
// -----------------------------------------------------------------------------

type IDataExchanger interface {
	// -----------------------------------------------------------------------------
	// Broadcast pushes data to external listeners or updates state.
	// We use interface{} to be generic (matching FastAPIServer implementation)
	Broadcast(payload interface{})

	// -----------------------------------------------------------------------------
	// AllDatas updates the internal state without broadcasting (matches Python)
	UpdateAllDatas(data interface{})

	// -----------------------------------------------------------------------------
	// Start the server
	Start() error

	// -----------------------------------------------------------------------------
	// Stop the server gracefully
	Stop() error
}
