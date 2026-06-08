package models

type PricePotential struct {
	P75 float64
	P90 float64
}

type TargetMatrix map[string]map[string]PricePotential
