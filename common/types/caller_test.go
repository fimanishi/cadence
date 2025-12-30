// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCallerType_String(t *testing.T) {
	tests := []struct {
		name       string
		callerType CallerType
		want       string
	}{
		{"CLI", CallerTypeCLI, "cli"},
		{"UI", CallerTypeUI, "ui"},
		{"SDK", CallerTypeSDK, "sdk"},
		{"Internal", CallerTypeInternal, "internal"},
		{"Unknown", CallerTypeUnknown, "unknown"},
		{"Zero value", CallerType(0), "unknown"},
		{"Invalid", CallerType(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.callerType.String())
		})
	}
}

func TestParseCallerType(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  CallerType
	}{
		{"cli", "cli", CallerTypeCLI},
		{"ui", "ui", CallerTypeUI},
		{"sdk", "sdk", CallerTypeSDK},
		{"internal", "internal", CallerTypeInternal},
		{"unknown", "unknown", CallerTypeUnknown},
		{"empty", "", CallerTypeUnknown},
		{"invalid", "invalid", CallerTypeUnknown},
		{"uppercase", "CLI", CallerTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseCallerType(tt.input))
		})
	}
}

func TestWithCallerType(t *testing.T) {
	tests := []struct {
		name       string
		callerType CallerType
	}{
		{"CLI", CallerTypeCLI},
		{"UI", CallerTypeUI},
		{"SDK", CallerTypeSDK},
		{"Internal", CallerTypeInternal},
		{"Unknown", CallerTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = WithCallerType(ctx, tt.callerType)

			assert.Equal(t, tt.callerType, GetCallerType(ctx))
		})
	}
}

func TestGetCallerType(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want CallerType
	}{
		{
			name: "nil context",
			ctx:  nil,
			want: CallerTypeUnknown,
		},
		{
			name: "context without caller type",
			ctx:  context.Background(),
			want: CallerTypeUnknown,
		},
		{
			name: "context with zero value caller type",
			ctx:  WithCallerType(context.Background(), CallerType(0)),
			want: CallerType(0),
		},
		{
			name: "context with CLI caller type",
			ctx:  WithCallerType(context.Background(), CallerTypeCLI),
			want: CallerTypeCLI,
		},
		{
			name: "context with SDK caller type",
			ctx:  WithCallerType(context.Background(), CallerTypeSDK),
			want: CallerTypeSDK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, GetCallerType(tt.ctx))
		})
	}
}

func TestCallerTypeRoundTrip(t *testing.T) {
	tests := []CallerType{
		CallerTypeCLI,
		CallerTypeUI,
		CallerTypeSDK,
		CallerTypeInternal,
		CallerTypeUnknown,
	}

	for _, ct := range tests {
		t.Run(ct.String(), func(t *testing.T) {
			str := ct.String()
			parsed := ParseCallerType(str)
			assert.Equal(t, ct, parsed)
		})
	}
}
