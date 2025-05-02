// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package failover

import (
	"context"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
)

type (
	// Notifier pushes failover event notifications
	Notifier interface {
		NotifyTaskLists(domains map[string]struct{})
	}

	notifierImpl struct {
		matchingClient matching.Client
		domainCache    cache.DomainCache
		logger         log.Logger
	}
)

func NewNotifier(
	matchingClient matching.Client,
	domainCache cache.DomainCache,
	logger log.Logger,
) Notifier {
	return &notifierImpl{
		matchingClient: matchingClient,
		domainCache:    domainCache,
		logger:         logger,
	}
}

func (n *notifierImpl) NotifyTaskLists(domains map[string]struct{}) {
	for domain := range domains {
		domainEntry, err := n.domainCache.GetDomainByID(domain)
		if err != nil {
			n.logger.Error("Failed to get domain name", tag.Error(err))
			continue
		}

		fmt.Printf("Domain active cluster: %s\n", domainEntry.GetReplicationConfig().ActiveClusterName)

		fmt.Println("Domain to notify: ", domainEntry.GetInfo().Name)
		n.notifyTaskListsPerDomain(domainEntry.GetInfo().Name)
	}
}

func (n *notifierImpl) notifyTaskListsPerDomain(domain string) {
	req := &types.GetTaskListsByDomainRequest{
		Domain: domain,
	}

	spew.Dump(req)

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := n.matchingClient.GetTaskListsByDomain(ctx, req)
	if err != nil {
		fmt.Println("Failed to get task lists by domain: ", domain)
		fmt.Println("Error: ", err)
		n.logger.Error("Failed to get task lists by domain", tag.Error(err))
		return
	}

	spew.Dump(resp)

	for tasklist := range resp.ActivityTaskListMap {
		fmt.Println("Activity Task List: ", tasklist)
	}

	for tasklist := range resp.DecisionTaskListMap {
		fmt.Println("Decision Task List: ", tasklist)
	}
}
