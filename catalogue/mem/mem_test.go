package mem_test

import (
	"testing"

	"storage4/catalogue"
	"storage4/catalogue/cattest"
	"storage4/catalogue/mem"
	"storage4/clock"
)

func TestContract(t *testing.T) {
	cattest.Run(t, func(t *testing.T, clk clock.Clock, meta catalogue.Meta, o catalogue.Options) catalogue.Catalogue {
		return mem.New(clk, meta, o)
	})
}
