package ksql

import "strconv"

type (
	WindowExpression interface {
		Expression() (string, bool)
		Type() WindowType
	}

	WindowType         int
	WindowDurationUnit int

	window struct {
		typ WindowType
	}

	tumblingWindow struct {
		window
		unit TimeUnit
	}

	hoppingWindow struct {
		window
		size    TimeUnit
		advance TimeUnit
	}

	sessionWindow struct {
		window
		gap TimeUnit
	}

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

func NewTumblingWindow(unit TimeUnit) WindowExpression {
	return &tumblingWindow{
		window: window{typ: Tumbling},
		unit:   TimeUnit{Val: unit.Val, Unit: unit.Unit},
	}
}

func NewHoppingWindow(size, advance TimeUnit) WindowExpression {
	return &hoppingWindow{
		window:  window{typ: Hopping},
		size:    TimeUnit{Val: size.Val, Unit: size.Unit},
		advance: TimeUnit{Val: advance.Val, Unit: advance.Unit},
	}
}

func NewSessionWindow(gap TimeUnit) WindowExpression {
	return &sessionWindow{
		window: window{typ: Session},
		gap:    TimeUnit{Val: gap.Val, Unit: gap.Unit},
	}
}

func (w *window) Type() WindowType { return w.typ }
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

func (sw *tumblingWindow) Expression() (string, bool) {
	if sw.unit.Val <= 0 {
		return "", false
	}

	timeUnitStr := sw.serializeTimeUnit(sw.unit.Unit)
	if len(timeUnitStr) == 0 {
		return "", false
	}

	return "TUMBLING (SIZE " + strconv.FormatInt(sw.unit.Val, 10) + " " + timeUnitStr + ")", true

}

func (hw *hoppingWindow) Expression() (string, bool) {
	if hw.size.Val <= 0 || hw.advance.Val <= 0 {
		return "", false
	}

	sizeTimeUnit := hw.serializeTimeUnit(hw.size.Unit)
	if len(sizeTimeUnit) == 0 {
		return "", false
	}

	advanceTimeUnit := hw.serializeTimeUnit(hw.advance.Unit)
	if len(advanceTimeUnit) == 0 {
		return "", false
	}

	return "HOPPING (SIZE " + strconv.FormatInt(hw.size.Val, 10) + " " + sizeTimeUnit +
		", ADVANCE BY " + strconv.FormatInt(hw.advance.Val, 10) + " " + advanceTimeUnit + ")", true
}

func (sw *sessionWindow) Expression() (string, bool) {
	if sw.gap.Val <= 0 {
		return "", false
	}

	timeUnitStr := sw.serializeTimeUnit(sw.gap.Unit)
	if len(timeUnitStr) == 0 {
		return "", false
	}

	return "SESSION (" + strconv.FormatInt(sw.gap.Val, 10) + " " + timeUnitStr + ")", true
}
