package ksql

import (
	"errors"
	"strconv"
)

type (
	// WindowExpression - common contract for all window expressions
	WindowExpression interface {
		Expression

		Type() WindowType
	}
	// WindowType - specific type of the window expression
	WindowType int
	// WindowDurationUnit - specific time unit for the window duration
	WindowDurationUnit int

	// Window - base structure for all window expressions
	window struct {
		typ WindowType
	}

	// TumblingWindow, HoppingWindow, SessionWindow - specific window expressions
	tumblingWindow struct {
		window
		unit TimeUnit
	}

	// HoppingWindow - represents a window that advances by a specified time unit
	hoppingWindow struct {
		window
		size    TimeUnit
		advance TimeUnit
	}

	// SessionWindow - represents a session window with a gap
	sessionWindow struct {
		window
		gap TimeUnit
	}

	// TimeUnit - represents a time unit with a value and a unit type
	TimeUnit struct {
		Val  int64
		Unit WindowDurationUnit
	}
)

const (
	Tumbling = WindowType(iota)
	Hopping
	Session
)

const (
	Milliseconds = WindowDurationUnit(iota)
	Seconds
	Minutes
	Hours
	Days
)

// NewTumblingWindow creates a new TumblingWindow with the specified time unit
func NewTumblingWindow(unit TimeUnit) WindowExpression {
	return &tumblingWindow{
		window: window{typ: Tumbling},
		unit:   TimeUnit{Val: unit.Val, Unit: unit.Unit},
	}
}

// NewHoppingWindow creates a new HoppingWindow with the specified size and advance time units
func NewHoppingWindow(size, advance TimeUnit) WindowExpression {
	return &hoppingWindow{
		window:  window{typ: Hopping},
		size:    TimeUnit{Val: size.Val, Unit: size.Unit},
		advance: TimeUnit{Val: advance.Val, Unit: advance.Unit},
	}
}

// NewSessionWindow creates a new SessionWindow with the specified gap time unit
func NewSessionWindow(gap TimeUnit) WindowExpression {
	return &sessionWindow{
		window: window{typ: Session},
		gap:    TimeUnit{Val: gap.Val, Unit: gap.Unit},
	}
}

// Type returns the type of the window expression
func (w *window) Type() WindowType { return w.typ }

// serializeTimeUnit converts a WindowDurationUnit to its string representation
func (w *window) serializeTimeUnit(unit WindowDurationUnit) string {
	switch unit {
	case Milliseconds:
		return "MILLISECONDS"
	case Seconds:
		return "SECONDS"
	case Minutes:
		return "MINUTES"
	case Hours:
		return "HOURS"
	case Days:
		return "DAYS"
	default:
		return ""
	}
}

// Expression accumulates all applied settings and builds the string query for the tumbling window
func (sw *tumblingWindow) Expression() (string, error) {
	if sw.unit.Val <= 0 {
		return "", errors.New("tumbling window size must be greater than 0")
	}

	timeUnitStr := sw.serializeTimeUnit(sw.unit.Unit)
	if len(timeUnitStr) == 0 {
		return "", errors.New("invalid time unit for tumbling window")
	}

	return "WINDOW TUMBLING (SIZE " + strconv.FormatInt(sw.unit.Val, 10) + " " + timeUnitStr + ")", nil

}

// Expression accumulates all applied settings and builds the string query for the hopping window
func (hw *hoppingWindow) Expression() (string, error) {
	if hw.size.Val <= 0 {
		return "", errors.New("hopping window size must be greater than 0")
	}

	if hw.advance.Val <= 0 {
		return "", errors.New("hopping window advance must be greater than 0")
	}

	sizeTimeUnit := hw.serializeTimeUnit(hw.size.Unit)
	if len(sizeTimeUnit) == 0 {
		return "", errors.New("invalid time unit for hopping window size")
	}

	advanceTimeUnit := hw.serializeTimeUnit(hw.advance.Unit)
	if len(advanceTimeUnit) == 0 {
		return "", errors.New("invalid time unit for hopping window advance")
	}

	return "WINDOW HOPPING (SIZE " + strconv.FormatInt(hw.size.Val, 10) + " " + sizeTimeUnit +
		", ADVANCE BY " + strconv.FormatInt(hw.advance.Val, 10) + " " + advanceTimeUnit + ")", nil
}

// Expression accumulates all applied settings and builds the string query for the session window
func (sw *sessionWindow) Expression() (string, error) {
	if sw.gap.Val <= 0 {
		return "", errors.New("session window gap must be greater than 0")
	}

	timeUnitStr := sw.serializeTimeUnit(sw.gap.Unit)
	if len(timeUnitStr) == 0 {
		return "", errors.New("invalid time unit for session window gap")
	}

	return "WINDOW SESSION (" + strconv.FormatInt(sw.gap.Val, 10) + " " + timeUnitStr + ")", nil
}
