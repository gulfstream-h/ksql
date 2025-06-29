package kinds

type (
	ValueFormat int // Modes of data serializing
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
