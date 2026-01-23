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
	"go.uber.org/yarpc/yarpctest"
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
		{"Empty string", CallerType(""), ""},
		{"Invalid", CallerType("invalid"), "invalid"},
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
		{"custom value", "my-custom-tool", CallerType("my-custom-tool")},
		{"uppercase", "CLI", CallerType("CLI")},
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

func TestNewCallerInfo(t *testing.T) {
	tests := []struct {
		name       string
		callerType CallerType
		want       CallerType
	}{
		{
			name:       "CLI",
			callerType: CallerTypeCLI,
			want:       CallerTypeCLI,
		},
		{
			name:       "SDK",
			callerType: CallerTypeSDK,
			want:       CallerTypeSDK,
		},
		{
			name:       "Unknown",
			callerType: CallerTypeUnknown,
			want:       CallerTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := NewCallerInfo(tt.callerType)
			assert.NotNil(t, info)
			assert.Equal(t, tt.want, info.GetCallerType())
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
			info: NewCallerInfo(CallerTypeCLI),
			want: CallerTypeCLI,
		},
		{
			name: "SDK CallerInfo",
			info: NewCallerInfo(CallerTypeSDK),
			want: CallerTypeSDK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.info.GetCallerType())
		})
	}
}

func TestContextWithCallerInfo(t *testing.T) {
	tests := []struct {
		name       string
		callerType CallerType
		want       CallerType
	}{
		{
			name:       "CLI caller info",
			callerType: CallerTypeCLI,
			want:       CallerTypeCLI,
		},
		{
			name:       "SDK caller info",
			callerType: CallerTypeSDK,
			want:       CallerTypeSDK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = ContextWithCallerInfo(ctx, NewCallerInfo(tt.callerType))

			got := GetCallerInfoFromContext(ctx)
			assert.NotNil(t, got)
			assert.Equal(t, tt.want, got.GetCallerType())
		})
	}
}

func TestContextWithCallerInfo_Nil(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithCallerInfo(ctx, nil)

	got := GetCallerInfoFromContext(ctx)
	assert.Nil(t, got)
}

func TestGetCallerInfoFromContext(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		wantNil    bool
		wantCaller CallerType
	}{
		{
			name:    "nil context",
			ctx:     nil,
			wantNil: true,
		},
		{
			name:    "context without caller info",
			ctx:     context.Background(),
			wantNil: true,
		},
		{
			name:       "context with CLI caller info",
			ctx:        ContextWithCallerInfo(context.Background(), NewCallerInfo(CallerTypeCLI)),
			wantNil:    false,
			wantCaller: CallerTypeCLI,
		},
		{
			name:       "context with SDK caller info",
			ctx:        ContextWithCallerInfo(context.Background(), NewCallerInfo(CallerTypeSDK)),
			wantNil:    false,
			wantCaller: CallerTypeSDK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetCallerInfoFromContext(tt.ctx)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tt.wantCaller, got.GetCallerType())
			}
		})
	}
}

func TestGetCallerInfoFromHeaders(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		wantNil    bool
		wantCaller CallerType
	}{
		{
			name:    "nil context",
			ctx:     nil,
			wantNil: true,
		},
		{
			name:    "context without YARPC call",
			ctx:     context.Background(),
			wantNil: true,
		},
		{
			name:       "YARPC call without caller-type header",
			ctx:        yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{}),
			wantNil:    false,
			wantCaller: CallerTypeUnknown,
		},
		{
			name: "YARPC call with CLI caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "cli"},
			}),
			wantNil:    false,
			wantCaller: CallerTypeCLI,
		},
		{
			name: "YARPC call with UI caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "ui"},
			}),
			wantNil:    false,
			wantCaller: CallerTypeUI,
		},
		{
			name: "YARPC call with SDK caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "sdk"},
			}),
			wantNil:    false,
			wantCaller: CallerTypeSDK,
		},
		{
			name: "YARPC call with internal caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "internal"},
			}),
			wantNil:    false,
			wantCaller: CallerTypeInternal,
		},
		{
			name: "YARPC call with empty caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: ""},
			}),
			wantNil:    false,
			wantCaller: CallerTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetCallerInfoFromHeaders(tt.ctx)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tt.wantCaller, got.GetCallerType())
			}
		})
	}
}

func TestGetContextWithCallerInfoFromHeaders(t *testing.T) {
	tests := []struct {
		name           string
		ctx            context.Context
		wantCallerInfo bool
		wantCallerType CallerType
	}{
		{
			name:           "nil context",
			ctx:            nil,
			wantCallerInfo: false,
		},
		{
			name:           "context without YARPC call",
			ctx:            context.Background(),
			wantCallerInfo: false,
		},
		{
			name:           "YARPC call without caller-type header",
			ctx:            yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{}),
			wantCallerInfo: true,
			wantCallerType: CallerTypeUnknown,
		},
		{
			name: "YARPC call with CLI caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "cli"},
			}),
			wantCallerInfo: true,
			wantCallerType: CallerTypeCLI,
		},
		{
			name: "YARPC call with SDK caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: "sdk"},
			}),
			wantCallerInfo: true,
			wantCallerType: CallerTypeSDK,
		},
		{
			name: "YARPC call with empty caller-type header",
			ctx: yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{CallerTypeHeaderName: ""},
			}),
			wantCallerInfo: true,
			wantCallerType: CallerTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.ctx == nil {
				assert.Nil(t, GetContextWithCallerInfoFromHeaders(tt.ctx))
				return
			}

			resultCtx := GetContextWithCallerInfoFromHeaders(tt.ctx)
			assert.NotNil(t, resultCtx)

			callerInfo := GetCallerInfoFromContext(resultCtx)
			if tt.wantCallerInfo {
				assert.NotNil(t, callerInfo)
				assert.Equal(t, tt.wantCallerType, callerInfo.GetCallerType())
			} else {
				assert.Nil(t, callerInfo)
			}
		})
	}
}
