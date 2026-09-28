// Copyright (c) 2021, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// The ComponentManager deals with communications between configd and the
// VCI components, and with the components' status (active / running / stopped
// etc).

package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/danos/config/data"
	rfc7951data "github.com/danos/encoding/rfc7951/data"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/exec"
	"github.com/danos/yang/data/datanode"
	yang "github.com/danos/yang/schema"
	// Debian's real golang-dbus-dev (5.1.0, upstream module path
	// github.com/godbus/dbus/v5) installs its source under the plain
	// unversioned GOPATH path github.com/godbus/dbus, not .../v5 -- a
	// packaging quirk, not something upstream's own go.mod declares.
	"github.com/godbus/dbus"
)

type ConfigMultiplexerFn func([][]byte, ModelSet) (*data.Node, error)

// Needs to match configd: (*commitctx) LogCommitTime()
type commitTimeLogFn func(string, time.Time)

func log(output string) {
	for _, line := range strings.Split(output, "\n") {
		if len(line) == 0 {
			continue
		}
		fmt.Printf("VCI: %s\n", line)
	}
}

type OperationsManager interface {
	Dial() error
	SetConfigForModel(string, interface{}) error
	CheckConfigForModel(string, interface{}) error
	StoreConfigByModelInto(string, interface{}) error
	StoreStateByModelInto(string, interface{}) error
}

type ServiceManager interface {
	Close()
	IsActive(name string) (bool, error)
}

// ComponentManager encapsulates bus operations to/from components, and service
// queries against the components' service status.
type ComponentManager interface {
	OperationsManager
	ServiceManager

	GetComponentNSMappings() *ComponentMappings

	ComponentValidation(
		ModelSet,
		datanode.DataNode,
		commitTimeLogFn,
	) []error
	ComponentSetRunning(
		ModelSet,
		datanode.DataNode,
		*map[string]bool,
	) []*exec.Output
	ComponentSetRunningWithLog(
		ModelSet,
		datanode.DataNode,
		*map[string]bool,
		commitTimeLogFn,
	) []*exec.Output
	ComponentGetRunning(
		ModelSet,
		ConfigMultiplexerFn,
	) (*data.Node, error)
	ComponentGetState(
		ModelSet,
		datanode.DataNode,
		*rfc7951data.Tree,
		StateLogger,
	) (*rfc7951data.Tree, error)
}

type compMgr struct {
	OperationsManager
	ServiceManager

	compMappings *ComponentMappings
}

var _ OperationsManager = (*compMgr)(nil)
var _ ServiceManager = (*compMgr)(nil)
var _ ComponentManager = (*compMgr)(nil)

func NewCompMgr(
	opsMgr OperationsManager,
	svcMgr ServiceManager,
	m yang.ModelSet,
	mappings *ComponentMappings,
) *compMgr {

	return &compMgr{
		OperationsManager: opsMgr,
		ServiceManager:    svcMgr,
		compMappings:      mappings,
	}
}

func (cm *compMgr) GetComponentNSMappings() *ComponentMappings {
	return cm.compMappings
}

// listActiveModels returns the topologically sorted list of models
// that are active in the provided config.  If they have config but are
// not running, they will not be returned.
//
// Typical usage would be for getting a list of models to query for state.
func (cm *compMgr) listActiveModels(
	m ModelSet,
	config datanode.DataNode) []string {

	out := make([]string, 0)

	for _, modelName := range cm.compMappings.OrderedComponents() {
		comp := cm.compMappings.Component(modelName)
		active, err := cm.IsActive(comp.name)
		if err != nil {
			log(err.Error())
		}
		if !active {
			continue
		}
		out = append(out, modelName)
	}
	return out
}

// listActiveOrConfiguredModels returns the topologically sorted list of models
// that are active in the provided config.  Models that have config but are
// not active are returned as they may need to be activated eg for validation.
func (cm *compMgr) listActiveOrConfiguredModels(
	m ModelSet,
	config datanode.DataNode,
) []string {

	out := make([]string, 0)
	for _, modelName := range cm.compMappings.OrderedComponents() {
		comp := cm.compMappings.Component(modelName)

		active, err := cm.IsActive(comp.name)
		if err != nil {
			log(err.Error())
		}

		// Either the model has been activated by default or it has config.
		// Only query models in one of these two states.
		// NB: FilterTree() can impact performance, esp on low-powered devices
		//     such as SIADs.  So, only call if component isn't active.
		//     A future enhancement would be to do a single pass to extract
		//     all active namespaces as we only need to know if a service is
		//     configured or not.  Actual config is irrelevant.
		if active || comp.HasConfiguration(m, config) {
			out = append(out, modelName)
		}
	}
	return out
}

func (cm *compMgr) ComponentValidation(
	m ModelSet,
	dn datanode.DataNode,
	logFn commitTimeLogFn,
) []error {

	if err := cm.Dial(); err != nil {
		return []error{
			fmt.Errorf("Validation: Unable to initialise component comms: %s",
				err)}
	}

	var errs []error
	for _, modelName := range cm.listActiveOrConfiguredModels(m, dn) {
		startTime := time.Now()

		comp := cm.compMappings.Component(modelName)
		jsonTree := comp.FilterCheckTree(m, dn)

		err := cm.CheckConfigForModel(modelName, string(jsonTree))
		if err != nil {
			errs = append(errs, err)
		}
		if logFn != nil {
			logFn(fmt.Sprintf("Check %s", modelName), startTime)
		}
	}
	return errs
}

func (cm *compMgr) ComponentSetRunning(
	m ModelSet,
	dn datanode.DataNode,
	changedNSMap *map[string]bool,
) []*exec.Output {
	return cm.ComponentSetRunningWithLog(m, dn, changedNSMap, nil)
}

func (cm *compMgr) ComponentSetRunningWithLog(
	m ModelSet,
	dn datanode.DataNode,
	changedNSMap *map[string]bool,
	commitLogFn commitTimeLogFn,
) []*exec.Output {

	log("Set Running configuration:\n")

	var outs []*exec.Output

	if err := cm.Dial(); err != nil {
		ee := &exec.Output{Path: []string{""}, Output: err.Error()}
		outs = append(outs, ee)
		return outs
	}

	var changedComps map[string]bool
	if changedNSMap != nil {
		changedComps = make(map[string]bool, 0)
		for ns, _ := range *changedNSMap {
			changedComps[cm.compMappings.nsMap[ns]] = true
		}
		changedComps[cm.compMappings.DefaultComponent()] = true
	}

	for _, ordComp := range cm.compMappings.OrderedComponents() {
		if changedComps != nil {
			if _, ok := changedComps[ordComp]; !ok {
				log(fmt.Sprintf("\t'%s' hasn't changed.\n", ordComp))
				continue
			}
		}
		startTime := time.Now()
		comp := cm.compMappings.Component(ordComp)
		log(fmt.Sprintf("\t'%s' has changed.\n", ordComp))

		jsonTree := comp.FilterSetTree(m, dn)
		err := cm.SetConfigForModel(ordComp, string(jsonTree))
		if err != nil {
			fmt.Printf("Failed to run component provisioning for %s: %s\n",
				ordComp, err.Error())
			if e, ok := err.(dbus.Error); ok {
				new_out := &exec.Output{Path: []string{""},
					Output: fmt.Sprint(e)}
				outs = append(outs, new_out)
			}
		}

		if commitLogFn != nil {
			commitLogFn(fmt.Sprintf("Commit %s", ordComp), startTime)
		}
	}
	return outs
}

func (cm *compMgr) ComponentGetRunning(
	m ModelSet,
	cfgMuxFn ConfigMultiplexerFn,
) (*data.Node, error) {

	if err := cm.Dial(); err != nil {
		return nil,
			fmt.Errorf(
				"ComponentGetRunning: Unable to initialise component comms: %s",
				err)
	}

	var configs = make([][]byte, 0, len(cm.compMappings.Components()))

	for _, comp := range cm.compMappings.Components() {
		// Build up JSON config, then decode ...
		var jsonInput string
		err := cm.StoreConfigByModelInto(
			comp.name, &jsonInput)

		if err != nil {
			return nil, fmt.Errorf("Unable to get running config for %s: %s",
				comp.name, err)
		}
		configs = append(configs, []byte(jsonInput))
	}

	return cfgMuxFn(configs, m)
}

func (cm *compMgr) ComponentGetState(
	m ModelSet,
	dn datanode.DataNode,
	ft *rfc7951data.Tree,
	logger StateLogger,
) (*rfc7951data.Tree, error) {

	if err := cm.Dial(); err != nil {
		return nil,
			fmt.Errorf(
				"ComponentGetState: Unable to initialise component comms: %s",
				err)
	}

	allState := newRFC7951Merger(m, ft)

	for _, model := range cm.listActiveModels(m, dn) {
		compStartTime := time.Now()
		state := rfc7951data.TreeNew()
		err := cm.StoreStateByModelInto(model, state)
		if err != nil {
			// No error if component doesn't implement state.
			_, ok := err.(*mgmterror.OperationNotSupportedApplicationError)
			if ok {
				logStateEvent(logger, fmt.Sprintf("%s no state fn", model))
				continue
			}
			logStateEvent(logger, fmt.Sprintf("%s store fail: %s", model, err))
			continue
		}
		allState.merge(state)
		logStateTime(logger, fmt.Sprintf("  %s", model), compStartTime)
	}

	return allState.getTree(), nil
}
