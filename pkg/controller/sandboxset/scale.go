package sandboxset

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandbox-manager/consts"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/expectations"
	managerutils "github.com/openkruise/agents/pkg/utils/sandbox-manager"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ScaleSandboxes handles all scale operations including surge.
// Returns scaling bool and error.
// scaling=true means scaling is needed, caller should skip update phase.
// scaling=false means no scaling needed, caller can proceed to update phase.
func (r *Reconciler) ScaleSandboxes(
	ctx context.Context,
	sbs *agentsv1alpha1.SandboxSet,
	allSandboxes []*agentsv1alpha1.Sandbox,
	groups GroupedSandboxes,
	currentRevision string,
	updateRevision string,
	scaleUpSatisfied bool,
	scaleDownSatisfied bool,
) (bool, error) {
	log := logf.FromContext(ctx)

	// Calculate diffs including surge
	diffRes := calculateExpectationDiffs(sbs, allSandboxes, currentRevision, updateRevision)

	// Scale up including surge pods
	if diffRes.scaleUpNum > 0 {
		if !scaleUpSatisfied {
			log.Info("skip scale up for scaleUpExpectation is not satisfied")
			return true, nil
		}
		log.Info("scale up", "count", diffRes.scaleUpNum)
		err := r.scaleUp(ctx, diffRes.scaleUpNum, sbs, updateRevision)
		return true, err
	}

	// Scale down when surge drops or user reduces replicas
	if diffRes.scaleDownNum > 0 {
		if !scaleUpSatisfied || !scaleDownSatisfied {
			log.Info("skip scale down for scaleUpExpectation or scaleDownExpectation is not satisfied")
			return true, nil
		}
		log.Info("scale down", "count", diffRes.scaleDownNum)
		err := r.scaleDown(ctx, diffRes.scaleDownNum, sbs, groups)
		return true, err
	}

	return false, nil
}

// scaleUp is allowed when scaleUpExpectation is satisfied
func (r *Reconciler) scaleUp(ctx context.Context, count int, sbs *agentsv1alpha1.SandboxSet, revision string) error {
	log := logf.FromContext(ctx)
	log.Info("scale up", "count", count)
	successes, err := utils.DoItSlowly(count, initialBatchSize, func() error {
		created, err := r.createSandbox(ctx, sbs, revision)
		if err != nil {
			log.Error(err, "failed to create sandbox")
			return err
		}
		log.V(consts.DebugLogLevel).Info("sandbox created", "sandbox", klog.KObj(created))
		return nil
	})
	log.Info("scale up finished", "successes", successes, "fails", count-successes)
	return err
}

// scaleDown is allowed when both scaleUpExpectation and scaleDownExpectation are satisfied
func (r *Reconciler) scaleDown(ctx context.Context, count int, sbs *agentsv1alpha1.SandboxSet, groups GroupedSandboxes) error {
	log := logf.FromContext(ctx)
	controllerKey := GetControllerKey(sbs)
	lock := uuid.New().String()
	log.Info("scale down", "count", count)
	var toDelete []client.ObjectKey
	for _, snapshot := range append(groups.Creating, groups.Available...) {
		if count <= 0 {
			break
		}
		toDelete = append(toDelete, client.ObjectKeyFromObject(snapshot))
		count--
	}
	successes, err := utils.DoItSlowlyWithInputs(toDelete, initialBatchSize, func(key client.ObjectKey) error {
		scaleDownExpectation.ExpectScale(controllerKey, expectations.Delete, key.Name)
		err := r.scaleDownSandbox(ctx, key, lock)
		if err != nil {
			log.Error(err, "failed to scale down sandbox")
			scaleDownExpectation.ObserveScale(controllerKey, expectations.Delete, key.Name)
		}
		return err
	})
	log.Info("scale down finished", "success", successes, "fails", len(toDelete)-successes)
	return err
}

func (r *Reconciler) scaleDownSandbox(ctx context.Context, key client.ObjectKey, lock string) (err error) {
	log := logf.FromContext(ctx).WithValues("sandbox", key).V(consts.DebugLogLevel)
	sbx := &agentsv1alpha1.Sandbox{}
	log.Info("try to scale down sandbox")
	if err = r.Get(ctx, key, sbx); err != nil {
		return err
	}
	if sbx.Annotations[agentsv1alpha1.AnnotationLock] != "" && sbx.Annotations[agentsv1alpha1.AnnotationOwner] != consts.OwnerManagerScaleDown {
		log.Info("sandbox to be scaled down claimed before performed, skip")
		return errors.New("sandbox to be scaled down claimed before performed, skip")
	}
	managerutils.LockSandbox(sbx, lock, consts.OwnerManagerScaleDown)
	if err = r.Update(ctx, sbx); err != nil {
		return fmt.Errorf("failed to lock sandbox when scaling down: %s", err)
	}
	if err = r.Delete(ctx, sbx); err != nil {
		log.Error(err, "failed to delete sandbox")
		return err
	}
	log.Info("sandbox locked and deleted")
	r.Recorder.Eventf(sbx, corev1.EventTypeNormal, EventSandboxScaledDown, "Sandbox %s locked and deleted", klog.KObj(sbx))
	return nil
}
