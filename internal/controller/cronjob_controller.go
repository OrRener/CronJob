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

package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	renerv1 "github.com/OrRener/cronjob/api/v1"
	cron "github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=batch.rener.med.one,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.rener.med.one,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.rener.med.one,resources=cronjobs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile

func (r *CronJobReconciler) getCronJob(ctx context.Context, req ctrl.Request) (*renerv1.CronJob, error) {
	var cronJob renerv1.CronJob
	err := r.Get(ctx, req.NamespacedName, &cronJob)
	if err != nil {
		return nil, err
	}
	return &cronJob, nil
}

func (r *CronJobReconciler) parseCronSchedule(schedule string) (cron.Schedule, error) {
	// Parse the cron schedule string
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		logf.Log.Error(err, "Failed to parse cron schedule", "schedule", schedule)
		return nil, err
	}
	logf.Log.Info("Parsed cron schedule", "schedule", schedule)
	return sched, nil
}

func (r *CronJobReconciler) getLastRun(cronJob renerv1.CronJob) time.Time {

	// Get last run time or creation time
	var lastRun time.Time
	if cronJob.Status.LastScheduleTime != nil {
		lastRun = cronJob.Status.LastScheduleTime.Time
	} else {
		lastRun = cronJob.CreationTimestamp.Time
	}
	logf.Log.Info("Last run time", "lastRun", lastRun, "cronJob", cronJob.Name)
	return lastRun
}

func (r *CronJobReconciler) checkIfItsTimeToRun(schedule cron.Schedule, cronjob renerv1.CronJob) bool {
	// Get the last run time
	lastRun := r.getLastRun(cronjob)

	// Get the current time
	currentTime := time.Now()

	nextRun := schedule.Next(lastRun)
	if currentTime.After(nextRun) || currentTime.Equal(nextRun) {
		logf.Log.Info("It's time to run the job", "nextRun", nextRun, "currentTime", currentTime, "cronJob", cronjob.Name)
		return true
	}
	logf.Log.Info("Not time to run the job yet", "nextRun", nextRun, "currentTime", currentTime, "cronJob", cronjob.Name)
	return false
}

func (r *CronJobReconciler) updateCronJobStatus(ctx context.Context, cronjob *renerv1.CronJob) error {

	cronjob.Status.LastScheduleTime = &metav1.Time{Time: time.Now()}
	logf.Log.Info("Updating CronJob status", "cronJob", cronjob.Name, "lastScheduleTime", cronjob.Status.LastScheduleTime)
	// Update the CronJob status
	if err := r.Status().Update(ctx, cronjob); err != nil {
		logf.Log.Error(err, "Failed to update CronJob status", "cronJob", cronjob.Name)
		return fmt.Errorf("failed to update CronJob status: %w", err)
	}
	logf.Log.Info("CronJob status updated successfully", "cronJob", cronjob.Name)
	return nil
}

func (r *CronJobReconciler) runJob(ctx context.Context, cronJob *renerv1.CronJob) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cronJob.Name + "-",
			Namespace:    cronJob.Namespace,
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}

	// Set OwnerReference so job is deleted when CronJob is deleted
	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
		return err
	}

	// Create the Job
	if err := r.Client.Create(ctx, job); err != nil {
		return err
	}

	// Update CronJob status LastScheduleTime
	cronJob.Status.LastScheduleTime = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, cronJob); err != nil {
		return err
	}
	logf.Log.Info("Job created successfully", "jobName", job.Name, "cronJob", cronJob.Name)
	r.updateCronJobStatus(ctx, cronJob)
	return nil
}

func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// Fetch the CronJob instance
	cronJob, err := r.getCronJob(ctx, req)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cronJob == nil {
		// If the CronJob does not exist, return without error
		return ctrl.Result{}, nil
	}
	sched, err := r.parseCronSchedule(cronJob.Spec.Schedule)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Check if it's time to run the job
	if r.checkIfItsTimeToRun(sched, *cronJob) {
		// Run the job
		if err := r.runJob(ctx, cronJob); err != nil {
			logf.Log.Error(err, "Failed to run job for CronJob", "cronJob", cronJob.Name)
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&renerv1.CronJob{}).
		Owns(&batchv1.Job{}).
		Named("cronjob").
		Complete(r)
}
