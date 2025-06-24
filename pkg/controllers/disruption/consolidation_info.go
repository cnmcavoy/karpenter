package disruption

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func consolidationMessage(candidates []*Candidate, results pscheduling.Results, candidatePrice float64) string {
	msg := ""
	for _, candidate := range candidates {
		if len(msg) > 0 {
			msg += ", "
		}
		msg += fmt.Sprintf("Node %s/%s/%s/%s", candidate.NodePool.Name, candidate.Node.Labels[corev1.LabelInstanceTypeStable], candidate.capacityType, candidate.Name())
	}
	if len(results.NewNodeClaims) > 0 {
		msg += fmt.Sprintf(" with a total price of %.3f is being replaced", candidatePrice)
		for _, nc := range results.NewNodeClaims {
			msg += " with offerings: ["
			for _, ito := range nc.InstanceTypeOptions {
				for _, of := range ito.Offerings {
					msg += fmt.Sprintf(" %s/%s/%s:%.3f ", ito.Name, of.CapacityType(), of.Zone(), of.Price)
				}
			}

			msg += " ]"
		}
	} else {
		msg += " is underutilized and will be removed without replacement"
	}

	return msg
}
