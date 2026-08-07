// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package enginebinding

import "testing"

func TestSkipAnnotationOptsOut(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		value string
		want  bool
	}{
		{value: "", want: false},
		{value: "true", want: true},
		{value: "1", want: true},
		{value: "skip this pod", want: true},
		{value: "false", want: false},
		{value: "0", want: false},
		{value: "off", want: false},
		{value: "disabled", want: false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()
			if got := SkipAnnotationOptsOut(tc.value); got != tc.want {
				t.Fatalf("SkipAnnotationOptsOut(%q) = %t, want %t", tc.value, got, tc.want)
			}
		})
	}
}
