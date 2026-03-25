package utils

import (
	"fmt"
	"strings"
	"time"

	"market-observer/src/interfaces"

	"github.com/scmhub/calendar"
)

// TradingCalendar calculates trading days using scmhub/calendar.
type TradingCalendar struct {
	Calendar *calendar.Calendar
	Fallback bool
	Timezone *time.Location
	Logger   interfaces.Logger
}

// -----------------------------------------------------------------------------

func GetMicForSymbol(symbol string) string {
	// Simple mapping based on suffix to MIC code
	// See scmhub/calendar for supported MICs (ISO 10383)
	mic := "xnys" // Default US NYSE
	if strings.HasSuffix(symbol, ".L") {
		mic = "xlon"
	} else if strings.HasSuffix(symbol, ".PA") {
		mic = "xpar"
	} else if strings.HasSuffix(symbol, ".DE") {
		mic = "xfra"
	} else if strings.HasSuffix(symbol, ".AS") {
		mic = "xams"
	} else if strings.HasSuffix(symbol, ".BR") {
		mic = "xbru"
	} else if strings.HasSuffix(symbol, ".MI") {
		mic = "xmil"
	} else if strings.HasSuffix(symbol, ".MC") {
		mic = "xmad"
	} else if strings.HasSuffix(symbol, ".ST") {
		mic = "xsto"
	} else if strings.HasSuffix(symbol, ".CO") {
		mic = "xcse"
	} else if strings.HasSuffix(symbol, ".HE") {
		mic = "xhel"
	} else if strings.HasSuffix(symbol, ".VI") {
		mic = "xwbo"
	} else if strings.HasSuffix(symbol, ".SW") {
		mic = "xswx"
	} else if strings.HasSuffix(symbol, ".TO") {
		mic = "xtse"
	} else if strings.HasSuffix(symbol, ".V") {
		mic = "xtsx"
	} else if strings.HasSuffix(symbol, ".T") {
		mic = "xtks"
	} else if strings.HasSuffix(symbol, ".HK") {
		mic = "xhkg"
	} else if strings.HasSuffix(symbol, ".AX") {
		mic = "xasx"
	} else if strings.HasSuffix(symbol, ".KS") {
		mic = "xkrx"
	} else if strings.HasSuffix(symbol, ".TW") {
		mic = "xtai"
	} else if strings.HasSuffix(symbol, ".SS") {
		mic = "xshg"
	} else if strings.HasSuffix(symbol, ".SZ") {
		mic = "xshe"
	}
	return mic
}

// -----------------------------------------------------------------------------

func GetCalendarByMic(mic string, logger interfaces.Logger) *TradingCalendar {
	// scmhub/calendar.GetCalendar returns a calendar by MIC
	cal := calendar.GetCalendar(mic)
	if cal == nil {
		// Fallback to xnys if not found
		cal = calendar.GetCalendar("xnys")
	}

	if cal == nil {
		if logger != nil {
			logger.Warning(fmt.Sprintf("Failed to load calendar for MIC '%s' and fallback 'xnys'. Using simple fallback (Mon-Fri 09:30-16:00 UTC).", mic))
		}
		// Try load NY location for fallback
		nyLoc, _ := time.LoadLocation("America/New_York")
		if nyLoc == nil {
			nyLoc = time.UTC // Worst case
		}
		return &TradingCalendar{Fallback: true, Timezone: nyLoc, Logger: logger}
	}

	return &TradingCalendar{Calendar: cal, Fallback: false, Timezone: cal.Loc, Logger: logger}
}

// -----------------------------------------------------------------------------

func GetCalendar(symbol string, logger interfaces.Logger) *TradingCalendar {
	mic := GetMicForSymbol(symbol)
	return GetCalendarByMic(mic, logger)
}

// -----------------------------------------------------------------------------

func (tc *TradingCalendar) IsTradingDay(date time.Time) bool {
	// Normalize to timezone if available
	if tc.Timezone != nil {
		date = date.In(tc.Timezone)
	}

	if tc.Fallback {
		// Simple fallback: Mon-Fri
		weekday := date.Weekday()
		return weekday != time.Saturday && weekday != time.Sunday
	}
	// Library handles IsHoliday / IsBusinessDay
	return tc.Calendar.IsBusinessDay(date)
}

// -----------------------------------------------------------------------------

// IsOpenOnMinute checks if the market is open at a specific minute.
func (tc *TradingCalendar) IsOpenOnMinute(t time.Time) bool {
	// Normalize to timezone if available
	if tc.Timezone != nil {
		t = t.In(tc.Timezone)
	}

	if tc.Fallback {
		if !tc.IsTradingDay(t) {
			return false
		}

		hour := t.Hour()
		minute := t.Minute()

		// 9:30 - 16:00 NY Time
		if (hour > 9 || (hour == 9 && minute >= 30)) && hour < 16 {
			return true
		}
		return false
	}

	return tc.Calendar.IsOpen(t)
}

// -----------------------------------------------------------------------------

// GetNextOpenTime returns the next time the market opens after the given time.
func (tc *TradingCalendar) GetNextOpenTime(fromTime time.Time) time.Time {
	// Normalize
	if tc.Timezone != nil {
		fromTime = fromTime.In(tc.Timezone)
	}

	// 1. Library support (if available)
	// scmhub/calendar doesn't explicitly expose "NextOpen" easily without iteration in some versions.
	// We will iterate day by day.

	current := fromTime
	// Limit lookahead to 10 days to avoid infinite loops
	for i := 0; i < 10; i++ {
		// 1. Check if market opens later TODAY
		if tc.IsTradingDay(current) {
			// Determine open time (Assume 09:30 for US/Fallback, 08:00 for others as safe default)
			hour, min := 8, 0
			if tc.Fallback {
				hour, min = 9, 30
			}

			y, m, d := current.Date()
			openTime := time.Date(y, m, d, hour, min, 0, 0, tc.Timezone)

			if openTime.After(fromTime) {
				return openTime
			}
		}

		// 2. Move to NEXT day (start of day)
		current = current.AddDate(0, 0, 1)
		y, m, d := current.Date()
		current = time.Date(y, m, d, 0, 0, 0, 0, tc.Timezone)

		// 3. Check if this next day is a trading day
		if tc.IsTradingDay(current) {
			var hour, min int
			if tc.Fallback {
				hour, min = 9, 30
			} else {
				// General heuristic for library supported markets
				hour, min = 9, 0
			}
			return time.Date(y, m, d, hour, min, 0, 0, tc.Timezone)
		}
	}

	// If fail, return 24h later
	return fromTime.Add(24 * time.Hour)
}
