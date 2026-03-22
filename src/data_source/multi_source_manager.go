package datasource

import (
	"context"
	"fmt"
	"sync"

	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
)

// MultiSourceManager aggregates multiple IDataSource instances type
type MultiSourceManager struct {
	Sources    map[string]interfaces.IDataSource
	Logger     *logger.Logger
	mu         sync.RWMutex
	outputChan chan<- map[string][]models.MStockPrice // Send-only, managed by parent
	ctx        context.Context                        // Lifecycle context (derived)
	cancelFunc context.CancelFunc                     // To stop all sources
	wg         *sync.WaitGroup                        // Shared WaitGroup (ptr)
}

// -----------------------------------------------------------------------------

func NewMultiSourceManager(sources []interfaces.IDataSource, log *logger.Logger) *MultiSourceManager {
	m := &MultiSourceManager{
		Sources: make(map[string]interfaces.IDataSource),
		Logger:  log,
	}

	for _, s := range sources {
		m.Sources[s.Name()] = s
	}

	return m
}

// -----------------------------------------------------------------------------

// AddSource adds a new source and starts it if the manager is running
func (m *MultiSourceManager) AddSource(source interfaces.IDataSource) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := source.Name()
	if _, exists := m.Sources[name]; exists {
		return fmt.Errorf("source %s already exists", name)
	}

	m.Sources[name] = source
	m.Logger.Info("Added source: %s", name)

	// If Manager is already running, start the new source immediately
	if m.outputChan != nil && m.ctx != nil {
		m.wg.Add(1)
		if err := source.Start(m.ctx, m.outputChan, m.wg); err != nil {
			m.wg.Done()
			return fmt.Errorf("failed to start source %s: %v", name, err)
		}
		m.Logger.Info("Started source: %s", name)
	}

	return nil
}

// ctx helper to get current context (requires lock or careful usage)
// Actually we should store ctx in struct if we need it for AddSource
// Adding ctx to struct in previous chunk would be better, but I can use a getter or simple field logic.
// Let's assume m.cancelFunc implies a context exists... but we need the Context object itself.
// I will add ctx field in the next edit or assume I can get it.
// Wait, I can't restart a cancelled context.
// So Start() creates a new Context. I need to store it.
// Let's add `ctx context.Context` to struct in a separate replacement or implied.
// I'll adjust the Struct chunk to include `ctx`.

// -----------------------------------------------------------------------------

// RemoveSource stops and removes a source
func (m *MultiSourceManager) RemoveSource(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	source, exists := m.Sources[name]
	if !exists {
		return fmt.Errorf("source %s not found", name)
	}

	// Stop the source
	if err := source.Stop(); err != nil {
		m.Logger.Error("Error stopping source %s: %v", name, err)
	}

	delete(m.Sources, name)
	m.Logger.Info("Removed source: %s", name)
	return nil
}

// -----------------------------------------------------------------------------

// GetSource retrieves a source by name
func (m *MultiSourceManager) GetSource(name string) (interfaces.IDataSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	source, exists := m.Sources[name]
	if !exists {
		return nil, fmt.Errorf("source %s not found", name)
	}
	return source, nil
}

// -----------------------------------------------------------------------------

// GetAllSources returns a list of all sources
func (m *MultiSourceManager) GetAllSources() []interfaces.IDataSource {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]interfaces.IDataSource, 0, len(m.Sources))
	for _, s := range m.Sources {
		list = append(list, s)
	}
	return list
}

// -----------------------------------------------------------------------------

// Start starts all sources
func (m *MultiSourceManager) Start(parentCtx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		return fmt.Errorf("MultiSourceManager is already running")
	}

	// 1. Store Lifecycle Objects
	// Derive a context so we can stop the manager independently if needed
	ctx, cancel := context.WithCancel(parentCtx)
	m.ctx = ctx
	m.cancelFunc = cancel

	m.outputChan = outputChan
	m.wg = wg // Store pointer to shared WG

	// 2. Start Sources
	for _, src := range m.Sources {
		m.wg.Add(1)
		if err := src.Start(m.ctx, m.outputChan, m.wg); err != nil {
			m.Logger.Error("Failed to start source %s: %v", src.Name(), err)
			m.wg.Done()
			return err
		}
	}
	return nil
}

// Stop stops all sources gracefully by cancelling the internal context
func (m *MultiSourceManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx == nil {
		return nil // Already stopped
	}

	m.Logger.Info("Stopping MultiSourceManager...")

	// Cancel context -> Signals all sources to stop
	if m.cancelFunc != nil {
		m.cancelFunc()
	}

	m.cancelFunc = nil
	m.ctx = nil

	m.Logger.Info("MultiSourceManager Stopped.")
	return nil
}

// -----------------------------------------------------------------------------

// StartSource starts a specific source by name
func (m *MultiSourceManager) StartSource(name string) error {
	m.mu.RLock()
	source, exists := m.Sources[name]
	ctx := m.ctx
	outChan := m.outputChan
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("source %s not found", name)
	}
	if outChan == nil || ctx == nil {
		return fmt.Errorf("MultiSourceManager is not running")
	}

	return source.Start(ctx, outChan, m.wg)
}

// -----------------------------------------------------------------------------

// StopSource stops a specific source by name
func (m *MultiSourceManager) StopSource(name string) error {
	m.mu.RLock()
	source, exists := m.Sources[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("source %s not found", name)
	}

	return source.Stop()
}

// -----------------------------------------------------------------------------

// Name returns "MultiSourceManager"
func (m *MultiSourceManager) Name() string {
	return "MultiSourceManager"
}

// -----------------------------------------------------------------------------

// FetchInitialData fans out to all sources and merges results
func (m *MultiSourceManager) FetchInitialData() (map[string][]models.MStockPrice, error) {
	results := make(map[string][]models.MStockPrice)
	var mu sync.Mutex
	var wg sync.WaitGroup

	sources := m.GetAllSources() // Get snapshot

	for _, src := range sources {
		wg.Add(1)
		go func(s interfaces.IDataSource) {
			defer wg.Done()
			data, err := s.FetchInitialData()
			if err != nil {
				m.Logger.Error("One of the sources failed initial fetch: %v", err)
				return // Continue with other sources
			}
			mu.Lock()
			for k, v := range data {
				results[k] = v
			}
			mu.Unlock()
		}(src)
	}
	wg.Wait()
	return results, nil
}

// -----------------------------------------------------------------------------

// FetchUpdateData fans out to all sources for manual update trigger
func (m *MultiSourceManager) FetchUpdateData() (map[string][]models.MStockPrice, error) {
	results := make(map[string][]models.MStockPrice)
	var mu sync.Mutex
	var wg sync.WaitGroup

	sources := m.GetAllSources()

	for _, src := range sources {
		wg.Add(1)
		go func(s interfaces.IDataSource) {
			defer wg.Done()
			data, err := s.FetchUpdateData()
			if err != nil {
				m.Logger.Error("One of the sources failed update fetch: %v", err)
				return
			}
			mu.Lock()
			for k, v := range data {
				results[k] = v
			}
			mu.Unlock()
		}(src)
	}
	wg.Wait()
	return results, nil
}

// -----------------------------------------------------------------------------

// IsRealTime checks if all underlying sources are compatible.
func (m *MultiSourceManager) IsRealTime() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.Sources) == 0 {
		return false
	}

	var firstStatus bool
	var firstSource string
	initialized := false

	// Gather status for debugging
	sourceStatuses := make(map[string]bool)

	for name, s := range m.Sources {
		isRT := s.IsRealTime()
		sourceStatuses[name] = isRT

		if !initialized {
			firstStatus = isRT
			firstSource = name
			initialized = true
			continue
		}

		if isRT != firstStatus {
			// Log critical and exit
			m.Logger.Critical("Data Source Incompatibility Detected :\nSource '%s' has IsRealTime=%v, but source '%s' has IsRealTime=%v.\nProgram cannot continue with mixed Real-Time and Delayed sources as it causes data corruption.", firstSource, firstStatus, name, isRT)
			return false // Unreachable due to os.Exit in Critical, but satisfied signature
		}
	}

	return firstStatus
}

// -----------------------------------------------------------------------------

func (m *MultiSourceManager) UpdateSymbols(symbols []string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, src := range m.Sources {
		if err := src.UpdateSymbols(symbols); err != nil {
			m.Logger.Error("Failed to update symbols for a source: %v", err)
			return err
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
