// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package flags

import (
	"flag"
	"testing"
	"time"

	"github.com/hashicorp/nomad/ci"
	"github.com/stretchr/testify/require"
)

func TestFlagHelper_Pointers_Set(t *testing.T) {
	ci.Parallel(t)

	var (
		B BoolValue
		b bool = true

		F NonNegativeFloat64Value
		f *float64

		D DurationValue
		d time.Duration = 2 * time.Minute

		U UintValue
		u uint = 99
	)
	flagSet := flag.NewFlagSet("test", flag.PanicOnError)
	flagSet.Var(&B, "b", "bool")
	flagSet.Var(&F, "f", "float64")
	flagSet.Var(&D, "d", "duration")
	flagSet.Var(&U, "u", "uint")

	args := []string{"-b", "false", "-f", "0.5", "-d", "1m", "-u", "42"}
	err := flagSet.Parse(args)
	require.NoError(t, err)

	require.Equal(t, "false", B.String())
	B.Merge(&b)
	require.Equal(t, false, b)

	require.Equal(t, "0.5", F.String())
	F.Merge(&f)
	require.NotNil(t, f)
	require.Equal(t, 0.5, *f)

	require.Equal(t, "1m0s", D.String())
	D.Merge(&d)
	require.Equal(t, 1*time.Minute, d)

	require.Equal(t, "42", U.String())
	U.Merge(&u)
	require.Equal(t, uint(42), u)
}

func TestFlagHelper_Pointers_Ignored(t *testing.T) {
	ci.Parallel(t)

	var (
		B BoolValue
		b bool = true

		F NonNegativeFloat64Value
		f *float64

		D DurationValue
		d time.Duration = 2 * time.Minute

		U UintValue
		u uint = 99
	)
	flagSet := flag.NewFlagSet("test", flag.PanicOnError)
	flagSet.Var(&B, "b", "bool")
	flagSet.Var(&F, "f", "float64")
	flagSet.Var(&D, "d", "duration")
	flagSet.Var(&U, "u", "uint")

	var args []string
	err := flagSet.Parse(args)
	require.NoError(t, err)

	require.Equal(t, "false", B.String())
	B.Merge(&b)
	require.Equal(t, true, b)

	require.Equal(t, "0", F.String())
	F.Merge(&f)
	require.Nil(t, f)

	require.Equal(t, "0s", D.String())
	D.Merge(&d)
	require.Equal(t, 2*time.Minute, d)

	require.Equal(t, "0", U.String())
	U.Merge(&u)
	require.Equal(t, uint(99), u)
}

func TestFlagHelper_NonNegativeFloat64Value_Invalid(t *testing.T) {
	ci.Parallel(t)

	testCases := []string{"-1", "NaN", "Inf"}
	for _, tc := range testCases {
		var f NonNegativeFloat64Value
		err := f.Set(tc)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a finite float greater than or equal to 0")
	}
}
