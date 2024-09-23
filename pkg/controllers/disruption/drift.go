/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"errors"
	"fmt"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"math"
	"sigs.k8s.io/controller-runtime/pkg/log"
	nodeutil "sigs.k8s.io/karpenter/pkg/utils/node"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
	clock       clock.Clock
}

func NewDrift(clock clock.Clock, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		clock:       clock,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return c.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]map[v1.DisruptionReason]int, candidates ...*Candidate) (Command, scheduling.Results, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).LastTransitionTime.Time)
	})

	// Do a quick check through the candidates to see if they're empty.
	// For each candidate that is empty with a nodePool allowing its disruption
	// add it to the existing command.
	empty := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if len(candidate.reschedulablePods) > 0 {
			continue
		}
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		if disruptionBudgetMapping[candidate.nodePool.Name][d.Reason()] > 0 {
			empty = append(empty, candidate)
			disruptionBudgetMapping[candidate.nodePool.Name][d.Reason()]--
		}
	}
	// Disrupt all empty drifted candidates, as they require no scheduling simulations.
	if len(empty) > 0 {
		log.FromContext(ctx).V(1).Info(fmt.Sprintf("prioritizing empty drift candidates, %d nodes", len(empty)))
		return Command{
			candidates: empty,
		}, scheduling.Results{}, nil
	}

	unevictablePodsCache := map[string]int{}
	unevictablePods := func(candidate *Candidate) int {
		if count, ok := unevictablePodsCache[candidate.Node.Name]; ok {
			return count
		}
		pods, err := nodeutil.GetPods(ctx, d.kubeClient, candidate.Node)
		if err != nil {
			log.FromContext(ctx).V(1).Error(err, "listing pods on node")
			return math.MaxInt
		}
		count := lo.CountBy(pods, func(p *corev1.Pod) bool { return !podutil.IsDisruptable(p) && podutil.IsWaitingEviction(p, d.clock) })
		unevictablePodsCache[candidate.Node.Name] = count
		return count
	}

	sort.SliceStable(candidates, func(a, b int) bool {
		return unevictablePods(candidates[a]) < unevictablePods(candidates[b])
	})

	for _, candidate := range candidates {
		count := unevictablePods(candidate)
		log.FromContext(ctx).V(1).Info("drift candidate", "node", candidate.Node.Name, "podsWaitingEvictionCount", count)

		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.nodePool.Name][d.Reason()] == 0 {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("drifted nodeclaim %s skipped, nodepool %s has no drift budget remaining", candidate.NodeClaim.Name, candidate.nodePool.Name))
			continue
		}
		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return Command{}, scheduling.Results{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("drifted nodeclaim %s skipped, %d pods could not be rescheduled", candidate.NodeClaim.Name, len(results.NonPendingPodSchedulingErrors())))
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, results.NonPendingPodSchedulingErrors())...)
			continue
		}

		log.FromContext(ctx).V(1).Info(fmt.Sprintf("returning drift candidate nodeclaim %s, new node claims: %+v", candidate.NodeClaim.Name, results.NewNodeClaims))

		return Command{
			candidates:   []*Candidate{candidate},
			replacements: results.NewNodeClaims,
		}, results, nil
	}
	return Command{}, scheduling.Results{}, nil
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
