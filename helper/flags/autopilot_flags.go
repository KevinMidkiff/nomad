// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package flags

// These flag type implementations are provided to maintain autopilot command
// backwards compatibility.

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"time"
)

// BoolValue provides a flag value that's aware if it has been set.
type BoolValue struct {
	v *bool
}

// Merge will overlay this value if it has been set.
func (b *BoolValue) Merge(onto *bool) {
	if b.v != nil {
		*onto = *(b.v)
	}
}

// Set implements the flag.Value interface.
func (b *BoolValue) Set(v string) error {
	if b.v == nil {
		b.v = new(bool)
	}
	var err error
	*(b.v), err = strconv.ParseBool(v)
	return err
}

// String implements the flag.Value interface.
func (b *BoolValue) String() string {
	var current bool
	if b.v != nil {
		current = *(b.v)
	}
	return fmt.Sprintf("%v", current)
}

// NonNegativeFloat64Value provides a finite, non-negative float64 flag value
// that's aware if it has been set.
type NonNegativeFloat64Value struct {
	v *float64
}

// Merge will overlay this value if it has been set.
func (f *NonNegativeFloat64Value) Merge(onto **float64) {
	if f.v != nil {
		v := *f.v
		*onto = &v
	}
}

// Set implements the flag.Value interface.
func (f *NonNegativeFloat64Value) Set(v string) error {
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return err
	}
	if parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fmt.Errorf("must be a finite float greater than or equal to 0")
	}
	f.v = &parsed
	return nil
}

// String implements the flag.Value interface.
func (f *NonNegativeFloat64Value) String() string {
	var current float64
	if f.v != nil {
		current = *(f.v)
	}
	return fmt.Sprintf("%v", current)
}

// DurationValue provides a flag value that's aware if it has been set.
type DurationValue struct {
	v *time.Duration
}

// Merge will overlay this value if it has been set.
func (d *DurationValue) Merge(onto *time.Duration) {
	if d.v != nil {
		*onto = *(d.v)
	}
}

// Set implements the flag.Value interface.
func (d *DurationValue) Set(v string) error {
	if d.v == nil {
		d.v = new(time.Duration)
	}
	var err error
	*(d.v), err = time.ParseDuration(v)
	return err
}

// String implements the flag.Value interface.
func (d *DurationValue) String() string {
	var current time.Duration
	if d.v != nil {
		current = *(d.v)
	}
	return current.String()
}

// UintValue provides a flag value that's aware if it has been set.
type UintValue struct {
	v *uint
}

// Merge will overlay this value if it has been set.
func (u *UintValue) Merge(onto *uint) {
	if u.v != nil {
		*onto = *(u.v)
	}
}

// Set implements the flag.Value interface.
func (u *UintValue) Set(v string) error {
	if u.v == nil {
		u.v = new(uint)
	}

	parsed, err := strconv.ParseUint(v, 0, bits.UintSize)
	if err != nil {
		return err
	}

	*(u.v) = (uint)(parsed)
	return err
}

// String implements the flag.Value interface.
func (u *UintValue) String() string {
	var current uint
	if u.v != nil {
		current = *(u.v)
	}
	return fmt.Sprintf("%v", current)
}
