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
)

type CallerType int

const (
	CallerTypeUnknown CallerType = iota
	CallerTypeCLI
	CallerTypeUI
	CallerTypeSDK
	CallerTypeInternal
)

type callerTypeContextKey string

const callerTypeKey = callerTypeContextKey("caller-type")

func (c CallerType) String() string {
	switch c {
	case CallerTypeCLI:
		return "cli"
	case CallerTypeUI:
		return "ui"
	case CallerTypeSDK:
		return "sdk"
	case CallerTypeInternal:
		return "internal"
	default:
		return "unknown"
	}
}

// ParseCallerType converts a string to CallerType
func ParseCallerType(s string) CallerType {
	switch s {
	case "cli":
		return CallerTypeCLI
	case "ui":
		return CallerTypeUI
	case "sdk":
		return CallerTypeSDK
	case "internal":
		return CallerTypeInternal
	default:
		return CallerTypeUnknown
	}
}

// WithCallerType adds CallerType to context
func WithCallerType(ctx context.Context, callerType CallerType) context.Context {
	return context.WithValue(ctx, callerTypeKey, callerType)
}

// GetCallerType retrieves CallerType from context, returns CallerTypeUnknown if not set
func GetCallerType(ctx context.Context) CallerType {
	if ctx == nil {
		return CallerTypeUnknown
	}
	callerType, ok := ctx.Value(callerTypeKey).(CallerType)
	if !ok {
		return CallerTypeUnknown
	}
	return callerType
}
