package models

import (
	"bytes"
	"fmt"
)

type AnomalyType int

const (
	AnomalyNone        AnomalyType = 0
	AnomalyVolumeBurst AnomalyType = 1
	AnomalyAbsorption  AnomalyType = 2
)

func (a AnomalyType) String() string {
	switch a {
	case AnomalyVolumeBurst:
		return "BURST"
	case AnomalyAbsorption:
		return "ABSORPTION"
	default:
		return "NONE"
	}
}

func (a AnomalyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", a.String())), nil
}

func (a *AnomalyType) UnmarshalJSON(b []byte) error {
	str := string(bytes.Trim(b, `"`))
	switch str {
	case "BURST":
		*a = AnomalyVolumeBurst
	case "ABSORPTION":
		*a = AnomalyAbsorption
	default:
		*a = AnomalyNone
	}
	return nil
}
