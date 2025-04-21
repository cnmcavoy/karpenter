package provisioning

import (
	"strings"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func annotateNodeClaimWithNominatedPods(
	n *scheduling.NodeClaim,
	nodeClaim *v1.NodeClaim,
) {
	var str strings.Builder
	for _, pod := range n.Pods {
		str.WriteString(pod.Namespace)
		str.WriteString("/")
		str.WriteString(pod.Name)
		str.WriteString(",")
	}
	nominatedPods := str.String()
	if len(nominatedPods) > 4096 {
		nominatedPods = nominatedPods[:4096]
	}
	// avoid datarace and make a copy
	annotations := make(map[string]string, len(nodeClaim.Annotations)+1)
	for k, v := range nodeClaim.Annotations {
		annotations[k] = v
	}
	annotations["karpenter.indeed.com/nominated-pods"] = nominatedPods
	nodeClaim.Annotations = annotations
}
