package sandboxset

import (
	"context"
	"sort"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/expectations"
	"github.com/openkruise/agents/pkg/utils/sandboxutils"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/integer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type expectationDiffs struct {
	// Scale-related fields
	scaleUpNum   int // Number of sandboxes to scale up including surge
	scaleDownNum int // Number of sandboxes to scale down when surge drops

	// Update-related fields
	updateNum            int // Number of sandboxes to update
	updateMaxUnavailable int // Max unavailable during update
}

func intAbs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func isOppositeSigns(a, b int) bool {
	return (a > 0 && b < 0) || (a < 0 && b > 0)
}

func isSandboxAvailable(sbx *agentsv1alpha1.Sandbox) bool {
	if sbx.DeletionTimestamp != nil {
		return false
	}
	state, _ := sandboxutils.GetSandboxState(sbx)
	return state == agentsv1alpha1.SandboxStateAvailable
}

func calculateExpectationDiffs(
	sbs *agentsv1alpha1.SandboxSet,
	sandboxes []*agentsv1alpha1.Sandbox,
	currentRevision string,
	updateRevision string,
) expectationDiffs {
	replicas := int(sbs.Spec.Replicas)
	var partition, maxSurge, maxUnavailable int

	// Calculate partition using shared utility
	if sbs.Spec.UpdateStrategy.Partition != nil {
		pValue, _ := utils.CalculatePartitionReplicas(sbs.Spec.UpdateStrategy.Partition, sbs.Spec.Replicas)
		partition = pValue
	}

	// Parse maxSurge
	if sbs.Spec.UpdateStrategy.MaxSurge != nil {
		maxSurge, _ = intstrutil.GetScaledValueFromIntOrPercent(
			sbs.Spec.UpdateStrategy.MaxSurge,
			replicas,
			true,
		)
	}

	// Parse maxUnavailable with default 20%
	maxUnavailableIntOrStr := sbs.Spec.UpdateStrategy.MaxUnavailable
	if maxUnavailableIntOrStr == nil {
		defaultValue := intstrutil.FromString(agentsv1alpha1.DefaultSandboxSetMaxUnavailable)
		maxUnavailableIntOrStr = &defaultValue
	}
	maxUnavailable, _ = intstrutil.GetScaledValueFromIntOrPercent(
		maxUnavailableIntOrStr,
		replicas,
		maxSurge == 0,
	)

	// Count sandboxes by revision
	var newRevisionCount, oldRevisionCount int

	for _, sbx := range sandboxes {
		if sbx.Labels[agentsv1alpha1.LabelTemplateHash] == updateRevision {
			newRevisionCount++
		} else {
			oldRevisionCount++
		}
	}

	// Calculate update diff from both directions
	updateOldDiff := oldRevisionCount - partition
	updateNewDiff := newRevisionCount - (replicas - partition)

	// Block rollback when template unchanged
	if updateRevision == currentRevision {
		updateOldDiff = integer.IntMax(updateOldDiff, 0)
		updateNewDiff = integer.IntMin(updateNewDiff, 0)
	}

	// Calculate surge when maxSurge is enabled and we have opposite signs
	var useSurge int
	if maxSurge > 0 && isOppositeSigns(updateOldDiff, updateNewDiff) {
		// Take the smaller absolute value
		useSurge = integer.IntMin(intAbs(updateOldDiff), intAbs(updateNewDiff))
		// Cap at maxSurge
		useSurge = integer.IntMin(useSurge, maxSurge)
	}

	// Calculate expected counts with surge
	expectedTotalCount := replicas + useSurge
	currentTotalCount := len(sandboxes)

	// Calculate scaleUpNum
	var scaleUpNum int
	if num := expectedTotalCount - currentTotalCount; num > 0 {
		scaleUpNum = num
	}

	// Calculate scaleDownNum
	var scaleDownNum int
	if num := currentTotalCount - expectedTotalCount; num > 0 {
		scaleDownNum = num
	}

	// Choose smaller absolute value for updateNum
	var updateNum int
	if intAbs(updateOldDiff) <= intAbs(updateNewDiff) {
		updateNum = updateOldDiff
	} else {
		updateNum = -updateNewDiff
	}

	updateMaxUnavailable := maxUnavailable + len(sandboxes) - replicas

	return expectationDiffs{
		scaleUpNum:           scaleUpNum,
		scaleDownNum:         scaleDownNum,
		updateNum:            updateNum,
		updateMaxUnavailable: updateMaxUnavailable,
	}
}

func (r *Reconciler) updateSandboxes(
	ctx context.Context,
	sbs *agentsv1alpha1.SandboxSet,
	sandboxes []*agentsv1alpha1.Sandbox,
	currentRevision string,
	updateRevision string,
) error {
	log := logf.FromContext(ctx)

	// Check if update strategy is OnDelete
	if sbs.Spec.UpdateStrategy.Type == agentsv1alpha1.OnDeleteSandboxSetUpdateStrategyType {
		log.V(3).Info("UpdateStrategy is OnDelete, skip rolling update")
		return nil
	}

	// Calculate diffs
	diffRes := calculateExpectationDiffs(sbs, sandboxes, currentRevision, updateRevision)
	if diffRes.updateNum == 0 {
		return nil
	}

	// Find sandboxes that can be updated
	var waitUpdateIndexes []int
	for i, sbx := range sandboxes {
		// Skip claimed sandboxes
		if sbx.Labels[agentsv1alpha1.LabelSandboxIsClaimed] == "true" {
			continue
		}

		// Skip sandboxes being deleted
		if sbx.DeletionTimestamp != nil {
			continue
		}

		// Skip paused sandboxes
		state, _ := sandboxutils.GetSandboxState(sbx)
		if state == agentsv1alpha1.SandboxStatePaused {
			continue
		}

		// Check if needs update
		if sbx.Labels[agentsv1alpha1.LabelTemplateHash] != updateRevision {
			waitUpdateIndexes = append(waitUpdateIndexes, i)
		}
	}

	if len(waitUpdateIndexes) == 0 {
		return nil
	}

	// Sort sandboxes by state priority, then by age
	sort.SliceStable(waitUpdateIndexes, func(i, j int) bool {
		sbxI := sandboxes[waitUpdateIndexes[i]]
		sbxJ := sandboxes[waitUpdateIndexes[j]]

		stateI, _ := sandboxutils.GetSandboxState(sbxI)
		stateJ, _ := sandboxutils.GetSandboxState(sbxJ)

		priorityI := getStatePriority(stateI)
		priorityJ := getStatePriority(stateJ)

		if priorityI != priorityJ {
			return priorityI < priorityJ
		}

		// Same state, sort by age
		return sbxI.CreationTimestamp.Before(&sbxJ.CreationTimestamp)
	})

	// Limit by updateNum
	updateDiff := intAbs(diffRes.updateNum)
	if updateDiff < len(waitUpdateIndexes) {
		waitUpdateIndexes = waitUpdateIndexes[:updateDiff]
	}

	// Count current unavailable sandboxes
	var unavailableCount int
	for _, sbx := range sandboxes {
		if !isSandboxAvailable(sbx) {
			unavailableCount++
		}
	}

	// Limit by maxUnavailable
	var canUpdateCount int
	for _, idx := range waitUpdateIndexes {
		sbx := sandboxes[idx]

		if isSandboxAvailable(sbx) {
			// Updating available sandbox increases unavailable count
			if unavailableCount >= diffRes.updateMaxUnavailable {
				break
			}
			unavailableCount++
		}
		// If already unavailable, doesn't count against limit
		canUpdateCount++
	}

	if canUpdateCount < len(waitUpdateIndexes) {
		waitUpdateIndexes = waitUpdateIndexes[:canUpdateCount]
	}

	// Delete sandboxes for update
	controllerKey := GetControllerKey(sbs)
	for _, idx := range waitUpdateIndexes {
		sbx := sandboxes[idx]
		scaleDownExpectation.ExpectScale(controllerKey, expectations.Delete, sbx.Name)
		if err := r.Delete(ctx, sbx); err != nil {
			log.Error(err, "failed to delete sandbox for update", "sandbox", sbx.Name)
			scaleDownExpectation.ObserveScale(controllerKey, expectations.Delete, sbx.Name)
			return err
		}
		log.V(3).Info("deleted sandbox for update", "sandbox", sbx.Name)
	}

	log.Info("update finished", "deleted", len(waitUpdateIndexes))
	return nil
}

func getStatePriority(state string) int {
	switch state {
	case agentsv1alpha1.SandboxStateCreating:
		return 1
	case agentsv1alpha1.SandboxStateAvailable:
		return 2
	default:
		return 3
	}
}

func updateStatus(
	newStatus *agentsv1alpha1.SandboxSetStatus,
	sandboxes []*agentsv1alpha1.Sandbox,
	sbs *agentsv1alpha1.SandboxSet,
	updateRevision string,
) {
	// Count UpdatedReplicas
	var updatedCount int32
	for _, sbx := range sandboxes {
		if sbx.Labels[agentsv1alpha1.LabelTemplateHash] == updateRevision {
			updatedCount++
		}
	}
	newStatus.UpdatedReplicas = updatedCount

	// Calculate ExpectedUpdatedReplicas using shared utility
	partition := 0
	if sbs.Spec.UpdateStrategy.Partition != nil {
		pValue, _ := utils.CalculatePartitionReplicas(sbs.Spec.UpdateStrategy.Partition, sbs.Spec.Replicas)
		partition = pValue
	}
	newStatus.ExpectedUpdatedReplicas = sbs.Spec.Replicas - int32(partition)

	// Update CurrentRevision when ALL sandboxes have updateRevision
	// This indicates the rolling update has fully completed, not just reached partition target
	if updatedCount == newStatus.Replicas && newStatus.Replicas == sbs.Spec.Replicas {
		newStatus.CurrentRevision = updateRevision
	}
}
