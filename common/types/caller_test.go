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

func TestCallerInfo_WithCallerType(t *testing.T) {
	tests := []struct {
		name       string
		initial    *CallerInfo
		callerType CallerType
		want       *CallerInfo
	}{
		{
			name:       "nil CallerInfo",
			initial:    nil,
			callerType: CallerTypeCLI,
			want:       &CallerInfo{CallerType: CallerTypeCLI},
		},
		{
			name:       "existing CallerInfo",
			initial:    &CallerInfo{CallerType: CallerTypeSDK},
			callerType: CallerTypeCLI,
			want:       &CallerInfo{CallerType: CallerTypeCLI},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial.WithCallerType(tt.callerType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCallerInfo_GetCallerType(t *testing.T) {
	tests := []struct {
		name string
		info *CallerInfo
		want CallerType
	}{
		{
			name: "nil CallerInfo",
			info: nil,
			want: CallerTypeUnknown,
		},
		{
			name: "CLI CallerInfo",
			info: &CallerInfo{CallerType: CallerTypeCLI},
			want: CallerTypeCLI,
		},
		{
			name: "SDK CallerInfo",
			info: &CallerInfo{CallerType: CallerTypeSDK},
			want: CallerTypeSDK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.info.GetCallerType())
		})
	}
}

func TestWithCallerInfo(t *testing.T) {
	tests := []struct {
		name       string
		callerInfo *CallerInfo
		want       *CallerInfo
	}{
		{
			name:       "CLI caller info",
			callerInfo: &CallerInfo{CallerType: CallerTypeCLI},
			want:       &CallerInfo{CallerType: CallerTypeCLI},
		},
		{
			name:       "SDK caller info",
			callerInfo: &CallerInfo{CallerType: CallerTypeSDK},
			want:       &CallerInfo{CallerType: CallerTypeSDK},
		},
		{
			name:       "nil caller info",
			callerInfo: nil,
			want:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = WithCallerInfo(ctx, tt.callerInfo)

			got := GetCallerInfo(ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetCallerInfo(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want *CallerInfo
	}{
		{
			name: "nil context",
			ctx:  nil,
			want: nil,
		},
		{
			name: "context without caller info",
			ctx:  context.Background(),
			want: nil,
		},
		{
			name: "context with caller info",
			ctx:  WithCallerInfo(context.Background(), &CallerInfo{CallerType: CallerTypeCLI}),
			want: &CallerInfo{CallerType: CallerTypeCLI},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetCallerInfo(tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}
