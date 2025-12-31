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

type CallerInfo struct {
	CallerType CallerType
}

// WithCallerType creates a new CallerInfo with the given CallerType
func (c *CallerInfo) WithCallerType(callerType CallerType) *CallerInfo {
	if c == nil {
		return &CallerInfo{CallerType: callerType}
	}
	newInfo := *c
	newInfo.CallerType = callerType
	return &newInfo
}

// GetCallerType returns the CallerType, or CallerTypeUnknown if CallerInfo is nil
func (c *CallerInfo) GetCallerType() CallerType {
	if c == nil {
		return CallerTypeUnknown
	}
	return c.CallerType
}

type callerInfoContextKey string

const callerInfoKey = callerInfoContextKey("caller-info")

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

// WithCallerInfo adds CallerInfo to context
func WithCallerInfo(ctx context.Context, callerInfo *CallerInfo) context.Context {
	if callerInfo == nil {
		return ctx
	}
	return context.WithValue(ctx, callerInfoKey, callerInfo)
}

// GetCallerInfo retrieves CallerInfo from context, returns nil if not set
func GetCallerInfo(ctx context.Context) *CallerInfo {
	if ctx == nil {
		return nil
	}
	callerInfo, _ := ctx.Value(callerInfoKey).(*CallerInfo)
	return callerInfo
}
