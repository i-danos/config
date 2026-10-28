// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3, stage 3-A: compensation of a failed multi-component commit.
//
// This file adds an optional capability next to CheckedComponentManager; the
// existing ComponentManager and CheckedComponentManager contracts and
// behaviour are unchanged. The design is in A02-PHASE3-DESIGN.md (v7):
//
//   - every component call (forward, compensation, reconcile) goes through one
//     dispatch path that calls a DispatchRecorder BEFORE the call is made and
//     waits for the reply with a deadline (callBounded). A DBus call cannot be
//     recalled: hitting the deadline only stops the caller from waiting.
//   - on the first forward failure the failing component and the components
//     applied before it are re-applied with their previous running
//     configuration, in reverse order, within a time budget. The outcome of
//     every component is recorded in an ApplyReport.
//   - ComponentReconcile re-applies a set of components and reads the
//     component's configuration back where the component supports it.
//
// Persistence (the journal), the fence gate and the commit control flow live
// in configd; this package only calls the recorder it is given.

package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/danos/mgmterror"
	"github.com/danos/yang/data/datanode"
	"github.com/danos/yang/data/encoding"
	yang "github.com/danos/yang/schema"
)

// DispatchKind tells the recorder why a component is about to be called.
type DispatchKind string

const (
	KindForward      DispatchKind = "forward"
	KindCompensation DispatchKind = "compensation"
	KindReconcile    DispatchKind = "reconcile"
)

// DispatchRecorder is implemented by configd (journal + service identity). It
// is called synchronously around EVERY SetConfigForModel call made by the
// compensating entry points. AfterDispatch may run on a different goroutine
// than BeforeDispatch (it also runs for a call whose caller already stopped
// waiting), so implementations must be safe for concurrent use.
type DispatchRecorder interface {
	// BeforeDispatch persists the dispatch record and returns the monotonic
	// time (microseconds, MonotonicNowUs clock) stored in it. A non-nil error
	// means the record is NOT durable and the call must not be made.
	BeforeDispatch(component string, kind DispatchKind, seq uint64) (tRecordMonoUs int64, err error)
	// AfterDispatch is best effort (marks the record settled); an error is
	// ignored by the caller.
	AfterDispatch(component string, kind DispatchKind, seq uint64, callErr error)
}

// ComponentOutcome is the result for one component in one phase.
type ComponentOutcome string

const (
	OutcomeApplied             ComponentOutcome = "applied"
	OutcomeFailed              ComponentOutcome = "failed"
	OutcomeUnknown             ComponentOutcome = "outcome_unknown"
	OutcomeNotAttempted        ComponentOutcome = "not_attempted"
	OutcomeNotSent             ComponentOutcome = "not_sent"
	OutcomeCompensated         ComponentOutcome = "compensated"
	OutcomeCompensationFailed  ComponentOutcome = "compensation_failed"
	OutcomeCompensationSkipped ComponentOutcome = "compensation_skipped"
	// OutcomeCompensationUnverified: the compensation call returned success
	// for a component whose forward outcome was unknown. The reply is not
	// trusted as proof (an abandoned forward call may still be running), so
	// the component is never reported as compensated.
	OutcomeCompensationUnverified ComponentOutcome = "compensation_unverified"
)

// Verification is the read-back result of a reconcile call.
type Verification string

const (
	VerifyNone          Verification = ""
	VerifyMatched       Verification = "verified"
	VerifyNotVerifiable Verification = "not_verifiable" // component has no getter
	VerifyMismatch      Verification = "mismatch"
	VerifyError         Verification = "verify_error"
)

// DirtyClass is the reason class for a component whose final state is unknown
// or inconsistent. Which classes need a fence before the component may be
// called again is decided by configd.
type DirtyClass string

const (
	DirtyUnknown            DirtyClass = "unknown"
	DirtyUnsettled          DirtyClass = "unsettled"
	DirtyUnfenced           DirtyClass = "unfenced"
	DirtyCompensationFailed DirtyClass = "compensation_failed"
	DirtyUnverified         DirtyClass = "unverified"
	DirtyBootPartial        DirtyClass = "boot_partial"
	// DirtyBlocked: a reconcile did not touch the component (a prerequisite
	// did not reconcile, or the components could not be reached). The
	// component stays dirty; configd keeps its earlier dirty class.
	DirtyBlocked DirtyClass = "blocked"
)

// DirtyEntry names a component that is not known to be in its intended state.
type DirtyEntry struct {
	Component string
	Class     DirtyClass
	Reason    string
}

// ComponentResult is the outcome of one component in an ApplyReport.
type ComponentResult struct {
	Component string
	// Phase is the kind of dispatch that produced Forward (KindForward for
	// ComponentSetRunningCompensated, KindReconcile for ComponentReconcile).
	// It decides how Forward is classified into Clean / Dirty.
	Phase        DispatchKind
	Forward      ComponentOutcome
	Compensation ComponentOutcome // "" when no compensation was relevant
	Verified     Verification     // reconcile only
	// Repair marks a component that was applied only because configd forced
	// it (its configuration is not changed by this commit); its target equals
	// the previous state, so it is never compensated.
	Repair bool
	// Soft marks an opportunistic repair (CompensationPolicy.Opportunistic):
	// a Repair step whose non-applied outcome does not stop the forward phase
	// and is never compensated.
	Soft       bool
	Note       string // skip reason, "blocked by X", ...
	ForwardErr error
	CompErr    error
}

// ApplyReport is returned by every compensating entry point. It is never nil.
type ApplyReport struct {
	Results []ComponentResult // forward (or requested) order
	// Clean: final state known to equal the intended end state.
	Clean []string
	// Dirty: final state unknown or known inconsistent.
	Dirty []DirtyEntry
	// Notes: informational lines (dropped forced keys, ...).
	Notes []string
}

// CompensationPolicy carries the time bounds and test hooks. The zero value
// means "defaults" (see DefaultCompensationPolicy).
type CompensationPolicy struct {
	// SkipCompensation turns compensation off (boot-time first commit).
	SkipCompensation bool

	// Opportunistic names components that are applied as Repair steps in the
	// ordered forward loop (running config for an unchanged component), never
	// compensated, and whose failure does not fail the forward phase: the
	// loop goes on and the report carries the outcome (Soft). A name that is
	// changed or forced is not opportunistic; unknown names are ignored.
	Opportunistic []string

	ForwardCallDeadline time.Duration // per forward / reconcile call
	CompCallDeadline    time.Duration // per compensation call
	CompensationBudget  time.Duration // whole compensation phase
	SendWindow          time.Duration // record time to send time
	MaxRecordAttempts   int           // BeforeDispatch retries on SendWindow overrun
	VerifyAttempts      int           // reconcile Set + read-back rounds

	// Test hooks; nil means the real clocks.
	Now       func() time.Time
	MonoNowUs func() int64
}

// Default bounds of the design (A02-PHASE3-DESIGN.md section 3 and 11.1).
const (
	forwardCallDeadline = 30 * time.Second
	compCallDeadline    = 20 * time.Second
	compensationBudget  = 60 * time.Second
	sendWindow          = 2 * time.Second
	maxRecordAttempts   = 3
	verifyAttempts      = 3
)

// DefaultCompensationPolicy returns the policy with the design's constants.
func DefaultCompensationPolicy() CompensationPolicy {
	return CompensationPolicy{}.withDefaults()
}

func (p CompensationPolicy) withDefaults() CompensationPolicy {
	if p.ForwardCallDeadline <= 0 {
		p.ForwardCallDeadline = forwardCallDeadline
	}
	if p.CompCallDeadline <= 0 {
		p.CompCallDeadline = compCallDeadline
	}
	if p.CompensationBudget <= 0 {
		p.CompensationBudget = compensationBudget
	}
	if p.SendWindow <= 0 {
		p.SendWindow = sendWindow
	}
	if p.MaxRecordAttempts <= 0 {
		p.MaxRecordAttempts = maxRecordAttempts
	}
	if p.VerifyAttempts <= 0 {
		p.VerifyAttempts = verifyAttempts
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.MonoNowUs == nil {
		p.MonoNowUs = MonotonicNowUs
	}
	return p
}

// ErrSendWindow: the dispatch record is older than the send window at send
// time even after re-recording; the call was not made.
var ErrSendWindow = errors.New("dispatch record older than the send window; call not made")

// ErrUnsettled: an earlier call to the component has not returned yet; no
// second call is made while it is outstanding.
var ErrUnsettled = errors.New("an earlier call to the component has not settled")

// ErrJournal wraps a BeforeDispatch failure: the call was not made.
type ErrJournal struct{ Cause error }

func (e *ErrJournal) Error() string {
	return fmt.Sprintf("cannot record the dispatch (journal unavailable): %s", e.Cause)
}
func (e *ErrJournal) Unwrap() error { return e.Cause }

// deadlineError is what callBounded returns when the caller stopped waiting.
// It is a net.Error timeout so isOutcomeUnknown classifies it like a bus
// timeout.
type deadlineError struct{ d time.Duration }

func (e *deadlineError) Error() string {
	return fmt.Sprintf("component call deadline of %s exceeded", e.d)
}
func (e *deadlineError) Timeout() bool   { return true }
func (e *deadlineError) Temporary() bool { return true }

// pendingCall is a call whose caller stopped waiting. done is closed when the
// call finally returned; err is valid after that.
type pendingCall struct {
	done chan struct{}
	err  error
}

// startCall runs fn in a goroutine. pc must already be registered wherever it
// is looked up (the pending table) BEFORE startCall is called; after, if not
// nil, runs once the call has returned and pc.done is closed, on the same
// goroutine, so the completion goroutine can clear its own pending entry by
// the same pointer.
func startCall(pc *pendingCall, fn func() error, after func()) {
	go func() {
		pc.err = fn()
		close(pc.done)
		if after != nil {
			after()
		}
	}()
}

// waitCall waits at most deadline for pc. It returns the call's result and
// false when the call finished in time, or a *deadlineError and true when the
// caller stopped waiting (the call keeps running).
func waitCall(pc *pendingCall, deadline time.Duration) (error, bool) {
	t := time.NewTimer(deadline)
	defer t.Stop()
	select {
	case <-pc.done:
		return pc.err, false
	case <-t.C:
		// Prefer a result that arrived at the same moment.
		select {
		case <-pc.done:
			return pc.err, false
		default:
		}
		return &deadlineError{d: deadline}, true
	}
}

// callBounded runs fn in a goroutine and waits at most deadline for it. On
// time it returns fn's result and a nil pendingCall. On deadline it returns a
// *deadlineError and the pendingCall that completes when fn returns. fn is
// never cancelled: a DBus call cannot be recalled.
func callBounded(deadline time.Duration, fn func() error) (error, *pendingCall) {
	pc := &pendingCall{done: make(chan struct{})}
	startCall(pc, fn, nil)
	err, timedOut := waitCall(pc, deadline)
	if timedOut {
		return err, pc
	}
	return err, nil
}

// CompensatingComponentManager is the optional capability used by configd
// when it can journal its dispatches. It is separate from ComponentManager
// and CheckedComponentManager, which are unchanged.
type CompensatingComponentManager interface {
	// ComponentSetRunningCompensated applies the changed components (plus the
	// forced ones) in order. On the first failure the failing component and
	// the components applied before it are re-applied with prevCfg(). The
	// report is never nil.
	ComponentSetRunningCompensated(
		m ModelSet,
		newCfg datanode.DataNode,
		prevCfg func() datanode.DataNode,
		changedNSMap *map[string]bool,
		forced []string,
		policy CompensationPolicy,
		rec DispatchRecorder,
		commitLogFn CommitTimeLogFn,
	) (*ApplyReport, []error)

	// CompensateReport reverts the components that a previous successful
	// forward phase applied (Forward == applied and not Repair), in reverse
	// order, through the same bounded and recorded path. configd calls it
	// when a step after the component apply (runfile persist) fails.
	CompensateReport(
		m ModelSet,
		prevCfg func() datanode.DataNode,
		applied *ApplyReport,
		policy CompensationPolicy,
		rec DispatchRecorder,
		commitLogFn CommitTimeLogFn,
	) (*ApplyReport, []error)

	// ComponentReconcile applies the target configuration to the given
	// components in dependency order, one recorded call each, and reads the
	// component's configuration back where it supports that. Failures do not
	// stop the others, but a component whose prerequisite (in the given set)
	// did not reconcile is skipped as "blocked by"; a read-back compares the
	// configuration after schema normalisation (see sameConfig for the
	// limits). Components that are not Clean in the report are still dirty,
	// including those that were not touched (class "blocked"). (Added to the two
	// methods of the design's section 11 because section 11.1 requires
	// reconcile calls to go through the same recorded path.)
	ComponentReconcile(
		m ModelSet,
		target func() datanode.DataNode,
		cmpTarget func() datanode.DataNode,
		components []string,
		policy CompensationPolicy,
		rec DispatchRecorder,
		commitLogFn CommitTimeLogFn,
	) (*ApplyReport, []error)
}

var _ CompensatingComponentManager = (*compMgr)(nil)
var _ CheckedComponentManager = (*compMgr)(nil)

// pendingTable lives in compMgr (zero value usable).
type pendingTable struct {
	mu      sync.Mutex
	calls   map[string]*pendingCall
	nextSeq uint64
}

func (t *pendingTable) seq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextSeq++
	return t.nextSeq
}

func (t *pendingTable) get(c string) *pendingCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls[c]
}

func (t *pendingTable) set(c string, pc *pendingCall) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.calls == nil {
		t.calls = make(map[string]*pendingCall)
	}
	t.calls[c] = pc
}

func (t *pendingTable) clear(c string, pc *pendingCall) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.calls[c] == pc {
		delete(t.calls, c)
	}
}

// dispatchResult is what one recorded component call produced.
type dispatchResult struct {
	outcome       ComponentOutcome // applied | failed | outcome_unknown | not_sent
	err           error
	unsettledSkip bool
}

// dispatch makes ONE recorded, bounded call of SetConfigForModel:
//  1. wait (bounded) for an earlier unsettled call to the same component;
//  2. rec.BeforeDispatch (the call is not made if it fails) and check the
//     send window, re-recording up to MaxRecordAttempts times;
//  3. the call itself, bounded by deadline; AfterDispatch runs when it returns.
func (cm *compMgr) dispatch(
	component string,
	kind DispatchKind,
	payload string,
	deadline time.Duration,
	pol CompensationPolicy,
	rec DispatchRecorder,
) dispatchResult {

	if pc := cm.pending.get(component); pc != nil {
		t := time.NewTimer(deadline)
		select {
		case <-pc.done:
			t.Stop()
		case <-t.C:
			return dispatchResult{outcome: OutcomeNotSent, err: ErrUnsettled, unsettledSkip: true}
		}
	}

	seq := cm.pending.seq()
	var recErr error
	recorded := false
	for attempt := 0; attempt < pol.MaxRecordAttempts; attempt++ {
		t, err := rec.BeforeDispatch(component, kind, seq)
		if err != nil {
			return dispatchResult{outcome: OutcomeNotSent, err: &ErrJournal{Cause: err}}
		}
		if pol.MonoNowUs()-t <= pol.SendWindow.Microseconds() {
			recorded = true
			break
		}
		recErr = ErrSendWindow
	}
	if !recorded {
		return dispatchResult{outcome: OutcomeNotSent, err: recErr}
	}

	// Register the pending entry BEFORE the call starts; the completion
	// goroutine clears it by the same pointer once the call returned, whether
	// or not the caller is still waiting. No handshake between the two sides.
	pc := &pendingCall{done: make(chan struct{})}
	cm.pending.set(component, pc)
	startCall(pc, func() error {
		e := cm.SetConfigForModel(component, payload)
		rec.AfterDispatch(component, kind, seq, e)
		return e
	}, func() { cm.pending.clear(component, pc) })
	err, timedOut := waitCall(pc, deadline)
	if timedOut {
		return dispatchResult{outcome: OutcomeUnknown, err: err}
	}
	if err == nil {
		return dispatchResult{outcome: OutcomeApplied}
	}
	if isOutcomeUnknown(err) {
		return dispatchResult{outcome: OutcomeUnknown, err: err}
	}
	return dispatchResult{outcome: OutcomeFailed, err: err}
}

// componentSkipReason is the text after "compensation_skipped:".
const (
	skipPolicy    = "policy"
	skipJournal   = "journal"
	skipBudget    = "budget"
	skipUnsettled = "unsettled"
	skipInactive  = "service not active"
	skipNoSend    = "nothing was sent"
	skipDial      = "components not reachable"
)

func (cm *compMgr) todoFor(
	changedNSMap *map[string]bool,
	forced []string,
	opportunistic []string,
) (todo []string, repair, soft map[string]bool, notes []string) {

	changed := map[string]bool{}
	if changedNSMap != nil {
		for ns := range *changedNSMap {
			changed[cm.compMappings.nsMap[ns]] = true
		}
		changed[cm.compMappings.DefaultComponent()] = true
	}
	known := map[string]bool{}
	for _, c := range cm.compMappings.OrderedComponents() {
		known[c] = true
	}
	want := map[string]bool{}
	repair = map[string]bool{}
	seen := map[string]bool{}
	for _, f := range forced {
		if seen[f] {
			continue
		}
		seen[f] = true
		if !known[f] {
			notes = append(notes, fmt.Sprintf(
				"forced component %q is not part of the model set; dropped", f))
			continue
		}
		want[f] = true
		if changedNSMap != nil && !changed[f] {
			repair[f] = true
		}
	}
	soft = map[string]bool{}
	if changedNSMap != nil {
		for _, o := range opportunistic {
			if known[o] && !changed[o] && !want[o] {
				want[o] = true
				repair[o] = true
				soft[o] = true
			}
		}
	}
	for _, c := range cm.compMappings.OrderedComponents() {
		if changedNSMap == nil || changed[c] || want[c] {
			todo = append(todo, c)
		}
	}
	return todo, repair, soft, notes
}

func (cm *compMgr) ComponentSetRunningCompensated(
	m ModelSet,
	newCfg datanode.DataNode,
	prevCfg func() datanode.DataNode,
	changedNSMap *map[string]bool,
	forced []string,
	policy CompensationPolicy,
	rec DispatchRecorder,
	commitLogFn CommitTimeLogFn,
) (*ApplyReport, []error) {

	pol := policy.withDefaults()
	report := &ApplyReport{}
	if rec == nil {
		return report, []error{programmingError("a DispatchRecorder is required")}
	}
	if prevCfg == nil && !pol.SkipCompensation {
		return report, []error{programmingError(
			"prevCfg is required unless compensation is skipped")}
	}

	log("Set Running configuration (compensating):\n")
	todo, repair, soft, notes := cm.todoFor(changedNSMap, forced, pol.Opportunistic)
	report.Notes = notes
	for _, c := range todo {
		report.Results = append(report.Results, ComponentResult{
			Component: c, Phase: KindForward,
			Forward: OutcomeNotAttempted, Repair: repair[c], Soft: soft[c]})
	}

	if err := cm.Dial(); err != nil {
		report.finalize(pol)
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = fmt.Sprintf(
			"cannot reach the components to apply the configuration: %s; "+
				"no component was changed", err)
		return report, []error{e}
	}

	failed := -1
	// Opportunistic components whose own prerequisite was an opportunistic
	// repair that did not succeed (or was itself blocked) are skipped as
	// "blocked by <p>" (design 7.2 item 5), like ComponentReconcile does. Only
	// Soft results are skipped this way: a changed or forced component is
	// never gated by an independent one.
	softNotDone := map[string]bool{}
	for ix, c := range todo {
		if report.Results[ix].Soft {
			blocked := ""
			for _, p := range cm.compMappings.Prerequisites(c) {
				if softNotDone[p] {
					blocked = p
					break
				}
			}
			if blocked != "" {
				report.Results[ix].Note = "blocked by " + blocked
				softNotDone[c] = true
				continue
			}
		}
		startTime := time.Now()
		comp := cm.compMappings.Component(c)
		log(fmt.Sprintf("\t'%s' has changed.\n", c))
		payload := string(comp.FilterSetTree(m, newCfg))
		res := cm.dispatch(c, KindForward, payload, pol.ForwardCallDeadline, pol, rec)
		r := &report.Results[ix]
		r.Forward = res.outcome
		r.ForwardErr = res.err
		if res.unsettledSkip {
			r.Note = skipUnsettled
		}
		if commitLogFn != nil {
			commitLogFn(fmt.Sprintf("Commit %s", c), startTime)
		}
		if res.outcome != OutcomeApplied {
			if r.Soft {
				softNotDone[c] = true
				continue // opportunistic repair: reported, never fatal
			}
			failed = ix
			break
		}
	}
	if failed < 0 {
		report.finalize(pol)
		return report, nil
	}

	cm.compensate(m, prevCfg, report, failed, pol, rec, commitLogFn)
	report.finalize(pol)
	return report, []error{
		forwardFailureError(report, failed),
		compensationReportError(report),
	}
}

func (cm *compMgr) CompensateReport(
	m ModelSet,
	prevCfg func() datanode.DataNode,
	applied *ApplyReport,
	policy CompensationPolicy,
	rec DispatchRecorder,
	commitLogFn CommitTimeLogFn,
) (*ApplyReport, []error) {

	pol := policy.withDefaults()
	out := &ApplyReport{}
	if applied != nil {
		out.Results = append(out.Results, applied.Results...)
		out.Notes = append(out.Notes, applied.Notes...)
	}
	if rec == nil {
		return out, []error{programmingError("a DispatchRecorder is required")}
	}
	if prevCfg == nil && !pol.SkipCompensation {
		return out, []error{programmingError(
			"prevCfg is required unless compensation is skipped")}
	}
	if err := cm.Dial(); err != nil {
		// Nothing can be reverted, but the components that were applied are
		// still running the new configuration: they must end up in the dirty
		// set, not vanish from the final state.
		for ix := range out.Results {
			r := &out.Results[ix]
			if r.Forward == OutcomeApplied && !r.Repair {
				r.Compensation = OutcomeCompensationSkipped
				r.Note = skipDial
				r.CompErr = err
			}
		}
		out.finalize(pol)
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = fmt.Sprintf(
			"cannot reach the components to revert the configuration: %s; %s",
			err, out.CompensationSummary())
		return out, []error{e}
	}
	cm.compensate(m, prevCfg, out, -1, pol, rec, commitLogFn)
	out.finalize(pol)
	if out.allCompensated() {
		return out, nil
	}
	return out, []error{compensationReportError(out)}
}

// compensate re-applies the previous configuration. failedIx >= 0 is the
// failing component of a forward phase (compensated first when something may
// have reached it); the components with Forward == applied (not Repair) follow
// in reverse order. Results are updated in place.
func (cm *compMgr) compensate(
	m ModelSet,
	prevCfg func() datanode.DataNode,
	report *ApplyReport,
	failedIx int,
	pol CompensationPolicy,
	rec DispatchRecorder,
	commitLogFn CommitTimeLogFn,
) {
	var order []int
	if failedIx >= 0 {
		f := report.Results[failedIx]
		switch {
		case f.Repair:
			// A repair step's target already equals the previous state: it is
			// never sent prevCfg, not even when it is the failing component.
			// Its dirty state is left as the report classifies it (see finalize).
		case f.Forward == OutcomeFailed || f.Forward == OutcomeUnknown:
			order = append(order, failedIx)
		case f.Forward == OutcomeNotSent:
			report.Results[failedIx].Compensation = OutcomeCompensationSkipped
			if report.Results[failedIx].Note == "" {
				report.Results[failedIx].Note = skipNoSend
			}
		}
	}
	for ix := len(report.Results) - 1; ix >= 0; ix-- {
		r := report.Results[ix]
		if ix == failedIx || r.Forward != OutcomeApplied || r.Repair {
			continue
		}
		order = append(order, ix)
	}
	if len(order) == 0 {
		return
	}

	var prev datanode.DataNode
	havePrev := false
	start := pol.Now()
	for _, ix := range order {
		r := &report.Results[ix]
		skip := func(reason string) {
			r.Compensation = OutcomeCompensationSkipped
			r.Note = reason
		}
		if pol.SkipCompensation {
			skip(skipPolicy)
			continue
		}
		remaining := pol.CompensationBudget - pol.Now().Sub(start)
		if remaining <= 0 {
			skip(skipBudget)
			continue
		}
		comp := cm.compMappings.Component(r.Component)
		if active, err := cm.IsActive(comp.name); err == nil && !active {
			skip(skipInactive)
			continue
		}
		if !havePrev {
			prev = prevCfg()
			havePrev = true
		}
		deadline := pol.CompCallDeadline
		if remaining < deadline {
			deadline = remaining
		}
		startTime := time.Now()
		payload := string(comp.FilterSetTree(m, prev))
		res := cm.dispatch(r.Component, KindCompensation, payload, deadline, pol, rec)
		if commitLogFn != nil {
			commitLogFn(fmt.Sprintf("Compensate %s", r.Component), startTime)
		}
		switch res.outcome {
		case OutcomeApplied:
			if r.Forward == OutcomeUnknown {
				r.Compensation = OutcomeCompensationUnverified
			} else {
				r.Compensation = OutcomeCompensated
			}
		case OutcomeNotSent:
			if res.unsettledSkip {
				skip(skipUnsettled)
			} else {
				skip(skipJournal)
			}
			r.CompErr = res.err
		default:
			r.Compensation = OutcomeCompensationFailed
			r.CompErr = res.err
			if res.outcome == OutcomeUnknown {
				r.Note = "compensation call timed out"
			}
		}
	}
}

// finalize computes Clean and Dirty from the results.
func (r *ApplyReport) finalize(pol CompensationPolicy) {
	r.Clean = nil
	r.Dirty = nil
	for _, c := range r.Results {
		if c.Phase == KindReconcile {
			r.classifyReconcile(c)
			continue
		}
		if c.Forward == OutcomeNotAttempted {
			continue // nothing happened
		}
		if c.Forward == OutcomeNotSent && c.Note == skipUnsettled {
			// This call was not made, but an EARLIER call to the component
			// has not returned: its state is unknown, so it is never clean.
			r.Dirty = append(r.Dirty, DirtyEntry{c.Component, DirtyUnsettled,
				"an earlier call has not settled"})
			continue
		}
		if c.Verified != VerifyNone {
			switch c.Verified {
			case VerifyMatched, VerifyNotVerifiable:
				r.Clean = append(r.Clean, c.Component)
			default:
				r.Dirty = append(r.Dirty, DirtyEntry{c.Component, DirtyUnverified,
					"read-back " + string(c.Verified)})
			}
			continue
		}
		if c.Compensation == "" {
			switch c.Forward {
			case OutcomeApplied:
				if c.Repair {
					r.Clean = append(r.Clean, c.Component)
				}
				// A normally applied component is judged by the commit: it is
				// neither clean nor dirty here (the new config is intended).
			case OutcomeUnknown:
				r.Dirty = append(r.Dirty, DirtyEntry{c.Component, DirtyUnknown,
					"forward call timed out or was abandoned"})
			case OutcomeFailed, OutcomeNotSent:
				if c.Repair {
					// A repair that did not happen proves nothing: neither
					// clean nor newly dirty. An existing dirty entry stays.
					continue
				}
				// Nothing known to have changed in it.
				r.Clean = append(r.Clean, c.Component)
			}
			continue
		}
		switch {
		case c.Forward == OutcomeUnknown:
			r.Dirty = append(r.Dirty, DirtyEntry{c.Component, DirtyUnknown,
				"forward outcome unknown; compensation " + string(c.Compensation)})
		case c.Compensation == OutcomeCompensated:
			r.Clean = append(r.Clean, c.Component)
		case c.Compensation == OutcomeCompensationFailed:
			class := DirtyCompensationFailed
			if c.Note == "compensation call timed out" {
				class = DirtyUnknown
			}
			r.Dirty = append(r.Dirty, DirtyEntry{c.Component, class,
				errString(c.CompErr)})
		case c.Compensation == OutcomeCompensationSkipped:
			if c.Forward == OutcomeNotSent {
				r.Clean = append(r.Clean, c.Component) // never sent
				continue
			}
			class := DirtyCompensationFailed
			switch c.Note {
			case skipPolicy:
				class = DirtyBootPartial
			case skipJournal:
				class = DirtyUnfenced
			case skipUnsettled:
				class = DirtyUnsettled
			}
			r.Dirty = append(r.Dirty, DirtyEntry{c.Component, class,
				"compensation skipped: " + c.Note})
		}
	}
}

// classifyReconcile files one reconcile result. A reconcile starts from a dirty
// component, so everything that is not a confirmed repair leaves it dirty:
// a failed or timed-out call, a call that could not be recorded (unfenced), a
// component that was not touched because a prerequisite did not reconcile, and
// a read-back that did not match.
func (r *ApplyReport) classifyReconcile(c ComponentResult) {
	dirty := func(class DirtyClass, reason string) {
		r.Dirty = append(r.Dirty, DirtyEntry{c.Component, class, reason})
	}
	switch c.Forward {
	case OutcomeApplied:
		switch c.Verified {
		case VerifyMatched, VerifyNotVerifiable:
			r.Clean = append(r.Clean, c.Component)
		default:
			dirty(DirtyUnverified, "read-back "+string(c.Verified))
		}
	case OutcomeUnknown:
		dirty(DirtyUnknown, "reconcile call timed out or was abandoned")
	case OutcomeFailed:
		dirty(DirtyCompensationFailed, "reconcile call failed: "+errString(c.ForwardErr))
	case OutcomeNotSent:
		if c.Note == skipUnsettled {
			dirty(DirtyUnsettled, "an earlier call has not settled")
		} else {
			dirty(DirtyUnfenced, "reconcile call not sent: "+errString(c.ForwardErr))
		}
	case OutcomeNotAttempted:
		if c.Note != "" {
			dirty(DirtyBlocked, c.Note)
		}
	}
}

func (r *ApplyReport) allCompensated() bool {
	for _, c := range r.Results {
		if c.Forward != OutcomeApplied || c.Repair {
			continue
		}
		if c.Compensation != OutcomeCompensated {
			return false
		}
	}
	return true
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func programmingError(msg string) error {
	e := mgmterror.NewOperationFailedApplicationError()
	e.Message = "internal error: " + msg
	return e
}

// forwardFailureError describes the first failing component. Unlike the phase
// 2 text it does not say that components were "NOT rolled back": the
// compensation report error that follows it does.
func forwardFailureError(r *ApplyReport, ix int) error {
	f := r.Results[ix]
	var msg string
	switch f.Forward {
	case OutcomeNotSent:
		if errors.Is(f.ForwardErr, ErrUnsettled) {
			msg = fmt.Sprintf("component %s was not called by this commit: %s; "+
				"its state is unknown until that call returns",
				f.Component, f.ForwardErr)
		} else {
			msg = fmt.Sprintf("cannot record the dispatch for %s: %s; nothing was sent to it",
				f.Component, f.ForwardErr)
		}
	case OutcomeUnknown:
		msg = fmt.Sprintf("component %s failed to apply the configuration: %s; "+
			"OUTCOME UNKNOWN (timeout / no reply): the component may or may not have applied it",
			f.Component, f.ForwardErr)
	default:
		msg = fmt.Sprintf("component %s failed to apply the configuration: %s; "+
			"the component reported a failure and is not known to have applied it",
			f.Component, f.ForwardErr)
	}
	var notAttempted []string
	for _, c := range r.Results[ix+1:] {
		notAttempted = append(notAttempted, c.Component)
	}
	if len(notAttempted) > 0 {
		msg += "; components not attempted: " + strings.Join(notAttempted, ", ")
	}
	e := mgmterror.NewOperationFailedApplicationError()
	e.Message = msg
	return e
}

// compensationReportError renders the per-component table of section 8.
func compensationReportError(r *ApplyReport) error {
	e := mgmterror.NewOperationFailedApplicationError()
	e.Message = r.CompensationSummary()
	return e
}

// CompensationSummary renders what happened to each component that was
// touched, e.g. "compensation: frr: compensated; kdump: compensation_failed
// (cause); ...".
func (r *ApplyReport) CompensationSummary() string {
	var parts []string
	for _, c := range r.Results {
		if c.Forward == OutcomeNotAttempted {
			continue
		}
		var s string
		switch {
		case c.Compensation != "":
			s = fmt.Sprintf("%s: %s", c.Component, c.Compensation)
			if c.Note != "" {
				s += " (" + c.Note + ")"
			}
			if c.CompErr != nil {
				s += " (" + c.CompErr.Error() + ")"
			}
			if c.Forward == OutcomeUnknown {
				s += "; forward outcome unknown"
			}
		case c.Forward == OutcomeApplied && c.Repair:
			s = fmt.Sprintf("%s: repaired", c.Component)
		case c.Forward == OutcomeApplied:
			s = fmt.Sprintf("%s: applied", c.Component)
		default:
			s = fmt.Sprintf("%s: %s", c.Component, c.Forward)
		}
		parts = append(parts, s)
	}
	text := "compensation: " + strings.Join(parts, "; ")
	if r.allCompensated() && len(r.Dirty) == 0 {
		text += ". All applied components were reverted to the previous running configuration."
	} else {
		var dirty []string
		for _, d := range r.Dirty {
			dirty = append(dirty, d.Component)
		}
		text += ". Running configuration was NOT changed. The runtime state of: " +
			strings.Join(dirty, ", ") +
			" may differ from it; they will be re-applied by 'reconcile' or the next commit; " +
			restartReconcilePhrase + "."
	}
	return text
}

// restartReconcilePhrase: production startup runs the reconcile pass
// (design section 8), so a report that leaves dirty components says so.
const restartReconcilePhrase = "also attempted automatically when configd restarts, after the component is fenced"

// Prerequisites returns the components (model names) that must be applied
// before the given one: the transitive closure of the Before/After edges of
// the component definitions, in OrderedComponents order. Unknown names and
// components without prerequisites give an empty list.
func (cm *ComponentMappings) Prerequisites(component string) []string {
	have := cm.prerequisites[component]
	if len(have) == 0 {
		return nil
	}
	set := make(map[string]bool, len(have))
	for _, h := range have {
		set[h] = true
	}
	var out []string
	for _, c := range cm.orderedComponents {
		if set[c] {
			out = append(out, c)
		}
	}
	return out
}

func (cm *compMgr) ComponentReconcile(
	m ModelSet,
	target func() datanode.DataNode,
	cmpTarget func() datanode.DataNode,
	components []string,
	policy CompensationPolicy,
	rec DispatchRecorder,
	commitLogFn CommitTimeLogFn,
) (*ApplyReport, []error) {

	pol := policy.withDefaults()
	report := &ApplyReport{}
	if rec == nil || target == nil {
		return report, []error{programmingError(
			"a DispatchRecorder and a target configuration are required")}
	}
	todo, _, _, notes := cm.todoFor(nil, components, nil)
	report.Notes = notes
	// todoFor(nil, …) returns everything; keep only the requested ones.
	want := map[string]bool{}
	for _, c := range components {
		want[c] = true
	}
	var ordered []string
	for _, c := range todo {
		if want[c] {
			ordered = append(ordered, c)
		}
	}
	for _, c := range ordered {
		report.Results = append(report.Results,
			ComponentResult{Component: c, Phase: KindReconcile,
				Forward: OutcomeNotAttempted})
	}
	if err := cm.Dial(); err != nil {
		for ix := range report.Results {
			report.Results[ix].Note = skipDial
		}
		report.finalize(pol)
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = fmt.Sprintf("cannot reach the components: %s", err)
		return report, []error{e}
	}

	tgt := target()
	// The same document with the schema defaults left out. It is never sent:
	// it exists so the read-back comparison can tell "the component echoed a
	// default" from "the component has different configuration" (R14, see
	// sameConfigAllowDefaulted).
	cmpTgt := datanode.DataNode(nil)
	if cmpTarget != nil {
		cmpTgt = cmpTarget()
	}
	notDone := map[string]bool{}
	for ix, c := range ordered {
		r := &report.Results[ix]
		blocked := ""
		for _, p := range cm.compMappings.Prerequisites(c) {
			if notDone[p] {
				blocked = p
				break
			}
		}
		if blocked != "" {
			r.Note = "blocked by " + blocked
			notDone[c] = true
			continue
		}
		startTime := time.Now()
		comp := cm.compMappings.Component(c)
		payload := string(comp.FilterSetTree(m, tgt))
		cmpPayload := payload
		if cmpTgt != nil {
			cmpPayload = string(comp.FilterSetTree(m, cmpTgt))
		}
		cm.reconcileOne(m, r, c, payload, cmpPayload, pol, rec)
		if commitLogFn != nil {
			commitLogFn(fmt.Sprintf("Reconcile %s", c), startTime)
		}
		if r.Forward != OutcomeApplied ||
			(r.Verified != VerifyMatched && r.Verified != VerifyNotVerifiable) {
			notDone[c] = true
		}
	}
	report.finalize(pol)
	var errs []error
	for _, c := range report.Results {
		if c.Forward != OutcomeNotAttempted && c.Forward != OutcomeApplied {
			e := mgmterror.NewOperationFailedApplicationError()
			e.Message = fmt.Sprintf("component %s not reconciled: %s", c.Component, errString(c.ForwardErr))
			errs = append(errs, e)
		}
	}
	return report, errs
}

// reconcileOne applies payload and verifies it by reading the component back,
// repeating Set + read-back up to pol.VerifyAttempts times on a mismatch.
func (cm *compMgr) reconcileOne(
	m ModelSet,
	r *ComponentResult,
	component, payload, cmpPayload string,
	pol CompensationPolicy,
	rec DispatchRecorder,
) {
	for attempt := 1; attempt <= pol.VerifyAttempts; attempt++ {
		res := cm.dispatch(component, KindReconcile, payload, pol.ForwardCallDeadline, pol, rec)
		r.Forward = res.outcome
		r.ForwardErr = res.err
		if res.outcome != OutcomeApplied {
			if res.unsettledSkip {
				r.Note = skipUnsettled
			}
			return
		}
		var got string
		err := cm.StoreConfigByModelInto(component, &got)
		if err != nil {
			var ns *mgmterror.OperationNotSupportedApplicationError
			if errors.As(err, &ns) {
				r.Verified = VerifyNotVerifiable
				return
			}
			r.Verified = VerifyError
			r.ForwardErr = err
			return
		}
		same, nerr := sameConfigAllowDefaulted(m, payload, cmpPayload, got)
		if nerr != nil {
			// What the component returned (or what was sent) cannot be
			// interpreted through the schema: that is not a mismatch we can
			// act on, report it as a verification error.
			r.Verified = VerifyError
			r.ForwardErr = nerr
			return
		}
		if same {
			r.Verified = VerifyMatched
			return
		}
		r.Verified = VerifyMismatch
	}
}

// sameConfig compares two RFC 7951 documents after normalising both through
// the schema (parse into a data tree, encode again), so that equivalent
// spellings compare equal: unqualified and module-qualified top-level names,
// key order, and whatever else the schema canonicalises. Limits: it depends on
// both documents being valid for the model set (an invalid document is an
// error, not a mismatch), and two documents that the schema encodes
// differently but a component treats as equivalent still compare unequal.
//
// Ordering: lists and leaf-lists the schema declares "ordered-by system" are
// compared as sets of entries, because the schema encoder and the components
// enumerate them in no stable order. "ordered-by user" lists and leaf-lists
// are compared in order (see canonicalJSON). Duplicates are not collapsed.
func sameConfig(m ModelSet, a, b string) (bool, error) {
	na, err := normaliseConfig(m, a)
	if err != nil {
		return false, fmt.Errorf("cannot normalise the configuration sent: %s", err)
	}
	nb, err := normaliseConfig(m, b)
	if err != nil {
		return false, fmt.Errorf("cannot normalise the configuration read back: %s", err)
	}
	return na == nb, nil
}

// sameConfigAllowDefaulted compares the configuration that was sent with what
// the component read back, ignoring the parts of the sent document that only
// exist because the schema gives them a default AND that the component echoed
// back unchanged.
//
// Why this is not simply sameConfig: the document sent to a component is built
// from a union tree, which materialises every YANG default (that is what lets
// "show configuration all" display them). A component that has never been
// configured reports only what it actually has -- vrrp answers "{}" -- so a
// byte comparison of the two says "mismatch" forever and the component sits in
// the journal's dirty set permanently (R14).
//
// Why not simply stop sending the defaults: components read the payload as
// their configuration, and several require the containers to be present.
// Sending "{}" made vrrp fail with "Error: 'vyatta-interfaces-v1:interfaces'"
// and kdump with "Error: No kernel crash dump configuration.", turning one
// stuck component into two failures. The document a component receives is not
// the thing that is wrong; the comparison is.
//
// stripped is the same document built from a tree with defaults removed: the
// difference between it and full is exactly the set of defaulted paths. Those
// paths are removed from the read-back too, but only where the read-back agrees
// with the default value -- a component that reports a different value there
// has real configuration and must still fail the comparison.
func sameConfigAllowDefaulted(m ModelSet, full, stripped, got string) (bool, error) {
	nf, err := normaliseConfig(m, full)
	if err != nil {
		return false, fmt.Errorf("cannot normalise the configuration sent: %s", err)
	}
	ns, err := normaliseConfig(m, stripped)
	if err != nil {
		return false, fmt.Errorf("cannot normalise the configuration sent: %s", err)
	}
	ng, err := normaliseConfig(m, got)
	if err != nil {
		return false, fmt.Errorf("cannot normalise the configuration read back: %s", err)
	}
	if nf == ns {
		return ng == ns, nil
	}
	return pruneAndCompare(nf, ns, ng)
}

// pruneAndCompare is the comparison itself, on already-normalised documents.
// Split out so it can be tested without a model set.
func pruneAndCompare(full, stripped, got string) (bool, error) {
	if full == stripped {
		return got == stripped, nil
	}
	var vf, vs, vg interface{}
	for _, p := range []struct {
		s string
		d *interface{}
	}{{full, &vf}, {stripped, &vs}, {got, &vg}} {
		dec := json.NewDecoder(strings.NewReader(p.s))
		dec.UseNumber()
		if err := dec.Decode(p.d); err != nil {
			return false, fmt.Errorf("cannot re-read the normalised configuration: %s", err)
		}
	}
	pruneDefaulted(vg, vf, vs)
	return canonEqual(vg, vs), nil
}

// pruneDefaulted walks the read-back and deletes every path that exists in the
// full document but not in the stripped one, when the read-back holds the same
// value the full document had there. Nodes whose value differs are left alone:
// the comparison must still see them.
func pruneDefaulted(got, full, stripped interface{}) {
	gm, ok := got.(map[string]interface{})
	if !ok {
		return
	}
	fm, fok := full.(map[string]interface{})
	sm, sok := stripped.(map[string]interface{})
	for k, gv := range gm {
		fv, hasFull := lookup(fm, fok, k)
		sv, hasStrip := lookup(sm, sok, k)
		if hasFull && !hasStrip {
			// Defaulted on the sent side: drop it if the component echoed it
			// unchanged, keep it (so it can be compared) if it did not.
			if canonEqual(gv, fv) {
				delete(gm, k)
			}
			continue
		}
		if hasFull && hasStrip {
			pruneDefaulted(gv, fv, sv)
		}
	}
}

func lookup(m map[string]interface{}, ok bool, k string) (interface{}, bool) {
	if !ok {
		return nil, false
	}
	v, has := m[k]
	return v, has
}

func canonEqual(a, b interface{}) bool {
	ba, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(ba) == string(bb)
}


func normaliseConfig(m ModelSet, doc string) (string, error) {
	dn, err := encoding.UnmarshalJSON(m, []byte(doc))
	if err != nil {
		return "", err
	}
	return canonicalJSON(m, encoding.ToRFC7951(m, dn))
}

// canonicalJSON returns a canonical spelling of a JSON document guided by the
// schema: object members are sorted by name (encoding/json does that for maps)
// and the elements of every list or leaf-list the schema declares
// "ordered-by system" are sorted by their own canonical spelling, because the
// schema layer and the components enumerate those in no stable order (A02
// stage 3-F, F4). The elements of an "ordered-by user" list or leaf-list keep
// their order: the order is part of the configuration, so the same entries in
// another order are a different configuration. An array whose schema node
// cannot be found is also kept in order, so a lookup failure can only produce
// a (reported) mismatch and never a false "matched".
func canonicalJSON(root yang.Node, doc []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	out, err := json.Marshal(canonicalValue(v, root))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// jsonMemberNode finds the schema node for a JSON member name, which may be
// module-qualified ("module:name"). It returns nil when there is none.
func jsonMemberNode(n yang.Node, name string) yang.Node {
	if n == nil {
		return nil
	}
	if i := strings.LastIndex(name, ":"); i >= 0 {
		name = name[i+1:]
	}
	c := n.Child(name)
	if c == nil {
		return nil
	}
	return c
}

func canonicalValue(v interface{}, n yang.Node) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, e := range t {
			t[k] = canonicalValue(e, jsonMemberNode(n, k))
		}
		return t
	case []interface{}:
		keys := make([]string, len(t))
		elems := make([]interface{}, len(t))
		// The members of a list entry are the children of the list's entry
		// node, not of the list node itself.
		member := n
		if _, ok := n.(yang.List); ok {
			member = n.Child("")
		}
		for i, e := range t {
			elems[i] = canonicalValue(e, member)
			b, _ := json.Marshal(elems[i])
			keys[i] = string(b)
		}
		if n == nil || n.OrdBy() == "user" {
			return elems
		}
		idx := make([]int, len(elems))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool { return keys[idx[a]] < keys[idx[b]] })
		sorted := make([]interface{}, len(elems))
		for i, j := range idx {
			sorted[i] = elems[j]
		}
		return sorted
	}
	return v
}
