/*
Copyright 2025.

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

package sandboxset

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/discovery"
	"github.com/openkruise/agents/pkg/features"
	"github.com/openkruise/agents/pkg/sandbox-manager/consts"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/expectations"
	utilfeature "github.com/openkruise/agents/pkg/utils/feature"
	"github.com/openkruise/agents/pkg/utils/fieldindex"
	stateutils "github.com/openkruise/agents/pkg/utils/sandboxutils"
)

func init() {
	flag.IntVar(&concurrentReconciles, "sandboxset-workers", concurrentReconciles, "Max concurrent workers for SandboxSet controller.")
	flag.IntVar(&initialBatchSize, "sandboxset-initial-batch-size", initialBatchSize, "The initial batch size to use for the api-server operation")
}

var (
	concurrentReconciles = 3
	initialBatchSize     = 16
	controllerKind       = agentsv1alpha1.GroupVersion.WithKind("SandboxSet")
)

func Add(mgr manager.Manager) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SandboxSetGate) || !discovery.DiscoverGVK(controllerKind) {
		return nil
	}
	err := (&Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	if err != nil {
		return err
	}
	klog.Infof("Started SandboxSetReconciler successfully")
	return nil
}

// Reconciler reconciles a Sandbox object
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Codec    runtime.Codec
}

const (
	EventSandboxCreated       = "SandboxCreated"
	EventCreateSandboxFailed  = "CreateSandboxFailed"
	EventSandboxScaledDown    = "SandboxScaledDown"
	EventFailedSandboxDeleted = "FailedSandboxDeleted"
)

// +kubebuilder:rbac:groups=agents.kruise.io,resources=sandboxsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agents.kruise.io,resources=sandboxsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.kruise.io,resources=sandboxsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	totalStart := time.Now()
	log := logf.FromContext(ctx).WithValues("sandboxset", req.NamespacedName)
	ctx = logf.IntoContext(ctx, log)
	sbs := &agentsv1alpha1.SandboxSet{}
	if err := r.Get(ctx, req.NamespacedName, sbs); err != nil {
		if apierrors.IsNotFound(err) {
			scaleUpExpectation.DeleteExpectations(req.String())
			scaleDownExpectation.DeleteExpectations(req.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Preparation
	newStatus, err := r.initNewStatus(sbs)
	if err != nil {
		log.Error(err, "failed to init new status")
		return ctrl.Result{}, err
	}

	controllerKey := GetControllerKey(sbs)
	var requeueAfter time.Duration
	var scaleUpSatisfied, scaleDownSatisfied bool
	scaleUpSatisfied, scaleUpTimeoutAfter := scaleExpectationSatisfied(ctx, scaleUpExpectation, controllerKey)
	scaleDownSatisfied, scaleDownTimeoutAfter := scaleExpectationSatisfied(ctx, scaleDownExpectation, controllerKey)
	requeueAfter = min(scaleUpTimeoutAfter, scaleDownTimeoutAfter)
	groups, err := r.groupAllSandboxes(ctx, sbs)
	if err != nil {
		log.Error(err, "failed to group sandboxes")
		return ctrl.Result{}, err
	}
	saveStatusFromGroup(newStatus, groups)

	// Set selector in status for scale subresource
	if newStatus.Selector == "" {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				agentsv1alpha1.LabelSandboxPool:      sbs.Name,
				agentsv1alpha1.LabelSandboxIsClaimed: "false",
			},
		})
		if err != nil {
			log.Error(err, "failed to generate selector")
		} else {
			newStatus.Selector = selector.String()
		}
	}

	var allErrors error

	// Collect all sandboxes for scale and update operations
	allSandboxes := append(groups.Creating, groups.Available...)
	allSandboxes = append(allSandboxes, groups.Used...)

	currentRevision := sbs.Status.CurrentRevision
	updateRevision := newStatus.UpdateRevision

	// Step 1: perform scale including surge
	start := time.Now()
	scaling, err := r.ScaleSandboxes(ctx, sbs, allSandboxes, groups, currentRevision, updateRevision, scaleUpSatisfied, scaleDownSatisfied)
	if err != nil {
		log.Error(err, "failed to perform scale", "cost", time.Since(start))
		allErrors = errors.Join(allErrors, err)
	} else {
		log.Info("scale finished", "cost", time.Since(start))
	}

	// Step 1: perform rolling update only if not scaling
	start = time.Now()
	if !scaling && scaleUpSatisfied && scaleDownSatisfied && sbs.DeletionTimestamp == nil {
		err = r.updateSandboxes(ctx, sbs, allSandboxes, currentRevision, updateRevision)
		if err != nil {
			log.Error(err, "failed to perform rolling update", "cost", time.Since(start))
			allErrors = errors.Join(allErrors, err)
		} else {
			log.Info("rolling update finished", "cost", time.Since(start))
		}

		// Calculate and update rolling update status
		updateStatus(newStatus, allSandboxes, sbs, updateRevision)
	}

	// Step 2: delete dead sandboxes
	start = time.Now()
	if err = r.deleteDeadSandboxes(ctx, groups.Dead); err != nil {
		log.Error(err, "failed to perform garbage collection")
		allErrors = errors.Join(allErrors, err)
	} else {
		log.Info("all dead sandboxes deleted", "cost", time.Since(start))
	}
	log.Info("reconcile done", "totalCost", time.Since(totalStart))
	if err = r.updateSandboxSetStatus(ctx, *newStatus, sbs); err != nil {
		log.Error(err, "failed to update sandboxset status")
		allErrors = errors.Join(allErrors, err)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, allErrors
}

func (r *Reconciler) createSandbox(ctx context.Context, sbs *agentsv1alpha1.SandboxSet, revision string) (*agentsv1alpha1.Sandbox, error) {
	generateName := fmt.Sprintf("%s-", sbs.Name)
	template := sbs.Spec.Template.DeepCopy()
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName,
			Namespace:    sbs.Namespace,
			Labels:       template.Labels,
			Annotations:  template.Annotations,
		},
		Spec: agentsv1alpha1.SandboxSpec{
			PersistentContents: sbs.Spec.PersistentContents,
			SandboxTemplate: agentsv1alpha1.SandboxTemplate{
				TemplateRef:          sbs.Spec.TemplateRef,
				Template:             template,
				VolumeClaimTemplates: sbs.Spec.VolumeClaimTemplates,
			},
		},
	}
	sbx.Annotations = clearAndInitInnerKeys(sbx.Annotations)
	sbx.Labels = clearAndInitInnerKeys(sbx.Labels)
	sbx.Labels[agentsv1alpha1.LabelSandboxPool] = sbs.Name
	sbx.Labels[agentsv1alpha1.LabelSandboxIsClaimed] = "false"
	sbx.Labels[agentsv1alpha1.LabelTemplateHash] = revision
	if err := ctrl.SetControllerReference(sbs, sbx, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, sbx); err != nil {
		r.Recorder.Eventf(sbs, corev1.EventTypeWarning, EventCreateSandboxFailed, "Failed to create sandbox: %s", err)
		return nil, err
	}
	scaleUpExpectation.ExpectScale(GetControllerKey(sbs), expectations.Create, sbx.Name)
	r.Recorder.Eventf(sbs, corev1.EventTypeNormal, EventSandboxCreated, "Sandbox %s created", klog.KObj(sbx))
	return sbx, nil
}

// deleteDeadSandboxes does not need to use ScaleExpectation, because this is a garbage collection logic that does not
// require maintaining replica counts (or rather, only needs to maintain the dead group's replica count at 0), so just
// delete all dead sandboxes.
func (r *Reconciler) deleteDeadSandboxes(ctx context.Context, dead []*agentsv1alpha1.Sandbox) error {
	log := logf.FromContext(ctx).V(consts.DebugLogLevel)
	failNum := 0
	for _, sbx := range dead {
		if sbx.DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, sbx); err != nil {
			log.Error(err, "failed to delete sandbox")
			failNum++
		}
		log.Info("sandbox deleted", "sandbox", klog.KObj(sbx))
		r.Recorder.Eventf(sbx, corev1.EventTypeNormal, EventFailedSandboxDeleted, "Sandbox %s deleted", klog.KObj(sbx))
	}
	if failNum > 0 {
		return fmt.Errorf("failed to delete %d sandboxes", failNum)
	}
	return nil
}

func (r *Reconciler) updateSandboxSetStatus(ctx context.Context, newStatus agentsv1alpha1.SandboxSetStatus, sbs *agentsv1alpha1.SandboxSet) error {
	log := logf.FromContext(ctx).V(consts.DebugLogLevel)
	clone := sbs.DeepCopy()
	if err := r.Get(ctx, client.ObjectKey{Namespace: sbs.Namespace, Name: sbs.Name}, clone); err != nil {
		log.Error(err, "failed to get updated sandboxset from client")
		return client.IgnoreNotFound(err)
	}
	if reflect.DeepEqual(clone.Status, newStatus) {
		return nil
	}
	clone.Status = newStatus
	err := r.Status().Update(ctx, clone)
	if err == nil {
		log.Info("update sandboxset status success", "status", utils.DumpJson(newStatus))
	} else {
		log.Error(err, "update sandboxset status failed")
	}
	return err
}

func (r *Reconciler) groupAllSandboxes(ctx context.Context, sbs *agentsv1alpha1.SandboxSet) (GroupedSandboxes, error) {
	log := logf.FromContext(ctx)
	sandboxList := &agentsv1alpha1.SandboxList{}
	if err := r.List(ctx, sandboxList,
		client.InNamespace(sbs.Namespace),
		client.MatchingFields{fieldindex.IndexNameForOwnerRefUID: string(sbs.UID)},
		client.UnsafeDisableDeepCopy,
	); err != nil {
		return GroupedSandboxes{}, err
	}
	groups := GroupedSandboxes{}
	for i := range sandboxList.Items {
		sbx := &sandboxList.Items[i]
		scaleUpExpectation.ObserveScale(GetControllerKey(sbs), expectations.Create, sbx.Name)
		debugLog := log.V(consts.DebugLogLevel).WithValues("sandbox", sbx.Name)
		state, reason := stateutils.GetSandboxState(sbx)
		switch state {
		case agentsv1alpha1.SandboxStateCreating:
			groups.Creating = append(groups.Creating, sbx)
		case agentsv1alpha1.SandboxStateAvailable:
			groups.Available = append(groups.Available, sbx)
		case agentsv1alpha1.SandboxStateRunning:
			fallthrough
		case agentsv1alpha1.SandboxStatePaused:
			groups.Used = append(groups.Used, sbx)
		case agentsv1alpha1.SandboxStateDead:
			groups.Dead = append(groups.Dead, sbx)
		default: // unknown, impossible, just in case
			return GroupedSandboxes{}, fmt.Errorf("cannot find state for sandbox %s", sbx.Name)
		}
		debugLog.Info("sandbox is grouped", "state", state, "reason", reason)
	}
	log.Info("sandbox group done", "total", len(sandboxList.Items), "creating", len(groups.Creating),
		"available", len(groups.Available), "used", len(groups.Used), "failed", len(groups.Dead))
	return groups, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	controllerName := "sandboxset-controller"
	r.Recorder = mgr.GetEventRecorderFor(controllerName)
	r.Codec = serializer.NewCodecFactory(mgr.GetScheme()).LegacyCodec(agentsv1alpha1.SchemeGroupVersion)
	return ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentReconciles}).
		Watches(&agentsv1alpha1.SandboxSet{}, &handler.EnqueueRequestForObject{}).
		Watches(&agentsv1alpha1.Sandbox{}, &SandboxEventHandler{}).
		Complete(r)
}
