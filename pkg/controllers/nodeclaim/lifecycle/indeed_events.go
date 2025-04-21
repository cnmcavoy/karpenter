package lifecycle

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

func InsufficientCapacityErrorEvents(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim, err error) []events.Event {
	errMsg := truncateMessage(err.Error())
	evnts := []events.Event{{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.InsufficientCapacityError,
		Message:        fmt.Sprintf("NodeClaim %s event: %s", nodeClaim.Name, errMsg),
		DedupeValues:   []string{string(nodeClaim.UID)},
	}}
	if nodeClaim.Annotations != nil && nodeClaim.Annotations["karpenter.indeed.com/nominated-pods"] != "" {
		awsZone := "any az"
		instanceTypes := ""

		for _, requirement := range nodeClaim.Spec.Requirements {
			if requirement.Key == "topology.kubernetes.io/zone" && len(requirement.Values) > 0 {
				awsZone = requirement.Values[0]
				break
			}
		}
		for _, requirement := range nodeClaim.Spec.Requirements {
			if requirement.Key == "node.kubernetes.io/instance-type" && len(requirement.Values) > 0 {
				instanceTypes = strings.Join(requirement.Values, ",")
				break
			}
		}

		pods := strings.Split(nodeClaim.Annotations["karpenter.indeed.com/nominated-pods"], ",")
		for _, podAndNS := range pods {
			split := strings.Split(podAndNS, "/")
			if len(split) != 2 {
				continue
			}
			pod := &corev1.Pod{}
			if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: strings.TrimSpace(split[0]), Name: strings.TrimSpace(split[1])}, pod); err == nil {
				evnts = append(evnts, events.Event{
					InvolvedObject: pod,
					Type:           corev1.EventTypeWarning,
					Reason:         events.InsufficientCapacityError,
					Message:        fmt.Sprintf("Pod could not schedule %s in %s: %s", instanceTypes, awsZone, errMsg),
					DedupeValues:   []string{events.InsufficientCapacityError + string(pod.UID)},
				})
			} else {
				if errors.IsNotFound(err) {
					continue
				}
				log.FromContext(ctx).Error(err, "Failed to get pod")
			}
		}
	}
	return evnts
}
