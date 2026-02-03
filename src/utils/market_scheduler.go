package utils

import (
	"sync"
	"time"

	"market-observer/src/logger"
)

type MarketScheduler struct {
	Calendars map[string]*TradingCalendar
	Logger    *logger.Logger
	mu        sync.RWMutex
}

// -----------------------------------------------------------------------------

func NewMarketScheduler(symbols []string, l *logger.Logger) *MarketScheduler {
	ms := &MarketScheduler{
		Calendars: make(map[string]*TradingCalendar),
		Logger:    l,
	}
	ms.MapSymbolsToCalendars(symbols)
	return ms
}

// -----------------------------------------------------------------------------

// MapSymbolsToCalendars maps a list of symbols to their respective calendars
func (ms *MarketScheduler) MapSymbolsToCalendars(symbols []string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Clear existing map
	ms.Calendars = make(map[string]*TradingCalendar)

	// Cache for reusing calendar pointers by MIC string
	micCache := make(map[string]*TradingCalendar)

	for _, symbol := range symbols {
		mic := GetMicForSymbol(symbol)

		// Check cache first
		if cal, exists := micCache[mic]; exists {
			ms.Calendars[symbol] = cal
		} else {
			// Create new and cache it
			cal := GetCalendarByMic(mic)
			if cal != nil {
				micCache[mic] = cal
				ms.Calendars[symbol] = cal
			}
		}
	}

	// Count unique calendars (should now match keys in micCache)
	uniqueCals := make(map[*TradingCalendar]bool)
	for _, cal := range ms.Calendars {
		uniqueCals[cal] = true
	}

	ms.Logger.Info("MarketScheduler: Mapped %d symbols to %d unique calendars.",
		len(symbols), len(uniqueCals))
}

// -----------------------------------------------------------------------------

// UpdateSymbols updates the scheduler with a new list of symbols
func (ms *MarketScheduler) UpdateSymbols(symbols []string) {
	ms.MapSymbolsToCalendars(symbols)
}

// -----------------------------------------------------------------------------

// AnyMarketOpen checks if ANY tracked markets are currently open
func (ms *MarketScheduler) AnyMarketOpen() bool {
	now := time.Now().UTC()

	// Get unique calendars
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	uniqueCals := make(map[*TradingCalendar]bool)
	for _, cal := range ms.Calendars {
		uniqueCals[cal] = true
	}

	// If no calendars, return false
	if len(uniqueCals) == 0 {
		return false
	}

	// Check each unique calendar
	for cal := range uniqueCals {
		open := cal.IsOpenOnMinute(now)
		if open {
			return true
		}
	}

	return false
}

// -----------------------------------------------------------------------------

// TimeUntilNextMarketOpen calculates the duration until the nearest market opens
func (ms *MarketScheduler) TimeUntilNextMarketOpen() time.Duration {
	now := time.Now().UTC()
	minDuration := 24 * time.Hour // Cap at 24 hours max wait

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	uniqueCals := make(map[*TradingCalendar]bool)
	for _, cal := range ms.Calendars {
		uniqueCals[cal] = true
	}

	if len(uniqueCals) == 0 {
		return minDuration
	}

	first := true

	for cal := range uniqueCals {
		nextOpen := cal.GetNextOpenTime(now)
		diff := nextOpen.Sub(now)

		if diff < 0 {
			diff = 0 // Should not happen if GetNextOpenTime is correct (future)
		}

		if first || diff < minDuration {
			minDuration = diff
			first = false
		}
	}

	return minDuration
}

// -----------------------------------------------------------------------------

// GetOpenSymbols returns a list of symbols whose markets are currently open.
func (ms *MarketScheduler) GetOpenSymbols() []string {
	now := time.Now().UTC()
	var openSymbols []string

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// We need to iterate SYMBOLS, not just calendars, because we need to return symbol names.
	// But our map is symbol -> calendar.
	for symbol, cal := range ms.Calendars {
		if cal.IsOpenOnMinute(now) {
			openSymbols = append(openSymbols, symbol)
		}
	}

	return openSymbols
}
