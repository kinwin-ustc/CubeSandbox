// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/ttrpc"
	"github.com/tencentcloud/CubeSandbox/Cubelet/api/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/Cubelet/api/services/errorcode/v1"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/recov"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/ret"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/cubelog"
)

func (s *service) Update(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{
		RequestID: req.RequestID,
		Ret:       &errorcode.Ret{RetCode: errorcode.ErrorCode_Success},
	}
	rt := &CubeLog.RequestTrace{
		Action:       "Update",
		RequestID:    req.RequestID,
		Caller:       constants.CubeboxServiceID.ID(),
		Callee:       s.engine.ID(),
		CalleeAction: "Update",
		InstanceID:   req.SandboxID,
	}
	ctx = CubeLog.WithRequestTrace(ctx, rt)
	log.G(ctx).Errorf("Update:%s", utils.InterfaceToString(req))
	start := time.Now()
	defer func() {
		if !ret.IsSuccessCode(rsp.Ret.RetCode) {
			log.G(ctx).WithFields(map[string]interface{}{
				"RetCode": int64(rsp.Ret.RetCode),
			}).Errorf("Update fail:%+v", rsp)
		}
		rt.Cost = time.Since(start)
		rt.RetCode = int64(rsp.Ret.RetCode)
		CubeLog.Trace(rt)
	}()

	if req.SandboxID == "" {
		rsp.Ret.RetMsg = "must provide container name"
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	}

	if req.Annotations == nil {
		rsp.Ret.RetMsg = "must provide Annotations"
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	}

	action := req.Annotations[constants.MasterAnnotationsUpdateAction]
	if action == "" {
		rsp.Ret.RetMsg = "must provide update action"
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	}
	rt.CalleeAction = action

	unlock := s.updateSandboxLocks.Lock(req.SandboxID)
	defer unlock()
	defer recov.HandleCrash(func(panicError interface{}) {
		log.G(ctx).Fatalf("Update panic info:%s, stack:%s", panicError, string(debug.Stack()))
		rsp.Ret.RetMsg = fmt.Sprintf("Update panic info:%s", panicError)
		rsp.Ret.RetCode = errorcode.ErrorCode_Unknown
	})

	sb, err := s.cubeboxMgr.cubeboxManger.Get(ctx, req.SandboxID)
	if err != nil {
		rsp.Ret.RetMsg = err.Error()
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	}
	rt.CalleeAction = action
	switch action {
	case constants.UpdateActionAddDevice, constants.UpdateActionRemoveDevice:
		rsp.Ret.RetMsg = "cloud disk hotplug is not supported in the open source build"
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	case constants.UpdateActionPause:
		return s.UpdateWithPause(ctx, req, sb)
	case constants.UpdateActionResume:
		return s.UpdateWithResume(ctx, req, sb)
	default:
		rsp.Ret.RetMsg = "invalid update action"
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		return rsp, nil
	}
}

func addPauseResumeMetaData(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest) context.Context {
	md, ok := ttrpc.GetMetadata(ctx)
	if !ok {
		md = ttrpc.MD{}
	}
	md.Append("pod_scope", req.SandboxID)
	ctx = ttrpc.WithMetadata(ctx, md)
	tmpmd, _ := ttrpc.GetMetadata(ctx)
	log.G(ctx).Debugf("metadata:%+v", tmpmd)
	return ctx
}

func (s *service) UpdateWithPause(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{
		RequestID: req.RequestID,
		Ret:       &errorcode.Ret{RetCode: errorcode.ErrorCode_Success},
	}
	if sb.GetStatus().IsPaused() {
		rsp.Ret.RetMsg = "sandbox is already paused"
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskStateInvalid
		return rsp, nil
	}
	if sb.GetStatus().IsTerminated() {
		// IsTerminated() covers both EXITED (FinishedAt!=0) and UNKNOWN
		// (Unknown=true). The legacy "sandbox is terminating" wording wrongly
		// implied a user-driven delete is in flight; use the same wording as
		// rollback.go's precheck so operators can tell the two states apart
		// from the message alone.
		rsp.Ret.RetMsg = "sandbox is not running"
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskStateInvalid
		return rsp, nil
	}

	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	ctx = constants.WithPreStopType(ctx, constants.PreStopTypePause)
	task, err := sb.FirstContainer().Container.Task(ctx, nil)
	if err != nil {
		rsp.Ret.RetMsg = err.Error()
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskPauseFailed
		return rsp, nil
	}
	log.G(ctx).Infof("UpdateWithPause:%s", utils.InterfaceToString(req))
	ctx = addPauseResumeMetaData(ctx, req)
	defer func() {

		s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID)
	}()
	defer utils.Recover()
	for _, c := range sb.AllContainers() {
		if c.Status != nil {
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausingAt = time.Now().UnixNano()
				return status, nil
			})
		}
	}

	for _, c := range sb.All() {
		doPreStop(ctx, c)
	}

	doPreStop(ctx, sb.FirstContainer())

	// Give task.Pause an explicit timeout so it cannot be stretched out
	// arbitrarily by the upstream ctx; otherwise, once the upstream ctx is
	// cancelled the cubelet view stays stuck at PAUSING while cubeshim is
	// already PAUSED.
	pauseCtx, pauseCancel := context.WithTimeout(ctx, taskPauseTimeout)
	defer pauseCancel()
	if pauseErr := task.Pause(pauseCtx); pauseErr != nil {
		// Even when ttrpc reports an error (DeadlineExceeded / canceled /
		// ttrpc closed), cubeshim may have actually paused the VM. Query the
		// real status once with an independent, ctx-immune short timeout and
		// persist the truth, so the state never stays stuck at PAUSING.
		reconcileStatusAfterPauseError(ctx, sb, task, pauseErr)
		rsp.Ret.RetMsg = pauseErr.Error()
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskPauseFailed
		return rsp, nil
	}
	for _, c := range sb.AllContainers() {
		if c.Status != nil {
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausedAt = time.Now().UnixNano()
				status.PausingAt = 0
				return status, nil
			})
		}
	}
	return rsp, nil
}

func (s *service) UpdateWithResume(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{
		RequestID: req.RequestID,
		Ret:       &errorcode.Ret{RetCode: errorcode.ErrorCode_Success},
	}
	if !sb.GetStatus().IsPaused() {
		rsp.Ret.RetMsg = "sandbox is not paused"
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskResumeFailed
		return rsp, nil
	}

	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	task, err := sb.FirstContainer().Container.Task(ctx, nil)
	if err != nil {
		rsp.Ret.RetMsg = err.Error()
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskResumeFailed
		return rsp, nil
	}
	log.G(ctx).Infof("UpdateWithResume:%s", utils.InterfaceToString(req))
	ctx = addPauseResumeMetaData(ctx, req)

	// 保证无论是否 panic，状态都会落盘
	defer func() {
		s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID)
	}()
	defer utils.Recover()

	resumeCtx, resumeCancel := context.WithTimeout(ctx, taskResumeTimeout)
	defer resumeCancel()
	if err := task.Resume(resumeCtx); err != nil {
		// Same as pause: resume may time out midway while cubeshim has already
		// brought the VM back to RUNNING. Query the real status once and
		// converge to the truth, so the state never stays stuck at PAUSED.
		reconcileStatusAfterResumeError(ctx, sb, task, err)
		rsp.Ret.RetMsg = err.Error()
		rsp.Ret.RetCode = errorcode.ErrorCode_TaskResumeFailed
		return rsp, nil
	}
	// CubeShim resumes paused VMs from an internal full snapshot under
	// /data/cubelet/root/pausevm/<sandbox> and does not expose that memory file
	// as a cubecow catalog entry. Any runtime/restore-base labels that still
	// point to older template/snapshot memory files are now stale for
	// pagemap_anon/soft-dirty purposes, so force the next commit to re-anchor
	// with a full snapshot.
	invalidateRuntimeSnapshotBindingsAfterOpaqueRestore(sb, time.Now().UTC())
	for _, c := range sb.AllContainers() {
		if c.Status != nil {
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausedAt = 0
				status.PausingAt = 0
				return status, nil
			})
		}
	}
	return rsp, nil
}

// Upper bound for the Pause/Resume ttrpc calls. 30s is used because cubeshim
// pausing a VM involves vCPU stop + device quiesce + memory eventual
// consistency, which is normally < 5s; 30s is a safety net to prevent the
// call from being stuck indefinitely when the upstream ctx is missing or
// blocked. Used together with the reconcile* error convergence.
const (
	taskPauseTimeout  = 30 * time.Second
	taskResumeTimeout = 30 * time.Second

	// Dedicated status-query timeout opened during reconcile. It MUST use a
	// fresh ctx and never reuse the already-expired ctx.
	reconcileStatusTimeout = 5 * time.Second
)

// reconcileStatusAfterPauseError, after task.Pause reports an error, actively
// queries cubeshim once for the real task status and straightens the cubelet
// in-memory view to the truth, so PausingAt never lingers forever. Note: all
// status writes here must stay consistent with the UpdateWithPause success
// path.
func reconcileStatusAfterPauseError(
	parentCtx context.Context,
	sb *cubeboxstore.CubeBox,
	task containerd.Task,
	pauseErr error,
) {
	// Deliberately start a fresh ctx from Background: parentCtx is very likely
	// already Done.
	queryCtx, cancel := context.WithTimeout(context.Background(), reconcileStatusTimeout)
	defer cancel()
	// Carry over the original ns to avoid namespaces.NamespaceRequired failing.
	if ns, ok := namespaces.Namespace(parentCtx); ok && ns != "" {
		queryCtx = namespaces.WithNamespace(queryCtx, ns)
	}

	st, qerr := task.Status(queryCtx)
	if qerr != nil {
		// Cannot determine the real status, so do not write blindly. Keep
		// PausingAt visible to operators and wait for the event-driven
		// reconcile (/tasks/paused subscription) to back it up.
		log.G(parentCtx).Errorf(
			"reconcileStatusAfterPauseError: task.Status failed sandbox=%s pauseErr=%v statusErr=%v",
			sb.ID, pauseErr, qerr)
		return
	}

	switch st.Status {
	case containerd.Paused:
		// cubeshim actually succeeded -> write PausedAt as in the success path.
		// Note: TaskPauseFailed is still returned to the upstream so it can
		// alert; but the cubelet internal state is consistent with the real VM,
		// and the next IsPaused() short-circuit becomes already-paused instead
		// of staying stuck at PAUSING forever.
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterPauseError: shim reports PAUSED despite pauseErr=%v, converging sandbox=%s",
			pauseErr, sb.ID)
		for _, c := range sb.AllContainers() {
			if c.Status == nil {
				continue
			}
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausedAt = time.Now().UnixNano()
				status.PausingAt = 0
				return status, nil
			})
		}
	case containerd.Running, containerd.Created:
		// Really not paused -> reset PausingAt to avoid it lingering forever.
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterPauseError: shim reports %s, rolling PausingAt back sandbox=%s pauseErr=%v",
			st.Status, sb.ID, pauseErr)
		for _, c := range sb.AllContainers() {
			if c.Status == nil {
				continue
			}
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausingAt = 0
				return status, nil
			})
		}
	default:
		// Intermediate states such as Stopped/Unknown/Pausing: leave the status
		// untouched and let TaskExit / the event subscription handle them.
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterPauseError: shim reports %s, leaving status untouched sandbox=%s",
			st.Status, sb.ID)
	}
}

// reconcileStatusAfterResumeError is the dual of the pause case.
func reconcileStatusAfterResumeError(
	parentCtx context.Context,
	sb *cubeboxstore.CubeBox,
	task containerd.Task,
	resumeErr error,
) {
	queryCtx, cancel := context.WithTimeout(context.Background(), reconcileStatusTimeout)
	defer cancel()
	if ns, ok := namespaces.Namespace(parentCtx); ok && ns != "" {
		queryCtx = namespaces.WithNamespace(queryCtx, ns)
	}

	st, qerr := task.Status(queryCtx)
	if qerr != nil {
		log.G(parentCtx).Errorf(
			"reconcileStatusAfterResumeError: task.Status failed sandbox=%s resumeErr=%v statusErr=%v",
			sb.ID, resumeErr, qerr)
		return
	}

	switch st.Status {
	case containerd.Running:
		// The shim has actually resumed successfully; likewise invalidate the
		// runtime snapshot bindings to stay consistent with the
		// UpdateWithResume success path.
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterResumeError: shim reports RUNNING despite resumeErr=%v, converging sandbox=%s",
			resumeErr, sb.ID)
		invalidateRuntimeSnapshotBindingsAfterOpaqueRestore(sb, time.Now().UTC())
		for _, c := range sb.AllContainers() {
			if c.Status == nil {
				continue
			}
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausedAt = 0
				status.PausingAt = 0
				return status, nil
			})
		}
	case containerd.Paused:
		// Really not resumed, the state stays PAUSED and needs no rewrite (the
		// success path has not run yet).
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterResumeError: shim still PAUSED resumeErr=%v sandbox=%s",
			resumeErr, sb.ID)
	default:
		log.G(parentCtx).Warnf(
			"reconcileStatusAfterResumeError: shim reports %s, leaving status untouched sandbox=%s",
			st.Status, sb.ID)
	}
}
