package kinds

type (
	ValueFormat int
)

const (
	JSON = ValueFormat(iota)
)

func (vf ValueFormat) String() string {
	switch vf {
	case JSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}
