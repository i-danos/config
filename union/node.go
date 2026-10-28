// Copyright (c) 2017-2021, AT&T Intellectual Property.
// All rights reserved.
//
// Copyright (c) 2015-2017 by Brocade Communications Systems, Inc.
// All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package union

import (
	"sort"

	"github.com/danos/config/data"
	"github.com/danos/config/schema"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/natsort"
	"github.com/danos/utils/pathutil"
	"github.com/danos/yang/data/datanode"
	yang "github.com/danos/yang/schema"
)

type Flags uint32

const (
	flagDeleted Flags = 1 << iota
	flagOpaque
	flagDefault
)

const (
	DontCheckAuth = false
	DontCare      = false
	CheckAuth     = true
)

type node struct {
	//the candidate tree node
	overlay *data.Node
	//the running tree node
	underlay *data.Node
	//the schema tree representation
	schema schema.Node
	//the parent base node
	parent Node
	//the specialized public node that this
	//base node represents
	specialized Node
	//flags representing what meta data about the
	//state of this node
	flags Flags
	//idx is the index of the child, this is currently
	//only filled out by the Children() method this is needed
	//for by-user sorting to track the relative order of children
	idx uint64
}

func newNode(overlay, underlay *data.Node, sch schema.Node, parent, spec Node, flags Flags) *node {
	return &node{
		overlay:     overlay,
		underlay:    underlay,
		schema:      sch,
		parent:      parent,
		flags:       flags,
		specialized: spec,
	}
}

//Sorting interface for lists of Nodes. ByUser handles the ordered-by
//user semantics from the Yang spec this stores the list in the order
//they were inserted by the user. BySystem does natural sort order.
type ByUser []Node

func (b ByUser) Len() int           { return len(b) }
func (b ByUser) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByUser) Less(i, j int) bool { return b[i].index() < b[j].index() }

type BySystem []Node

func (b BySystem) Len() int           { return len(b) }
func (b BySystem) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b BySystem) Less(i, j int) bool { return natsort.Less(b[i].Name(), b[j].Name()) }

func (n *node) getnode() *node {
	return n
}

func (n *node) merge(mergeFn func(Node) *data.Node, skipFn func(Node) bool) *data.Node {
	data := n.Data()
	if data == nil {
		//protect against bogus tree
		return nil
	}
	out := data.Copy()
	if n.def() {
		//record whether this is here for default
		//reason or not, to ensure it doesn't end up
		//as user defined in the effective tree during
		//commit. Currently this is the only use
		//of 'default' for the data tree.
		out.MarkDefault()
	}
	for _, ch := range n.SortedChildren() {
		datach := mergeFn(ch)
		if datach == nil {
			continue
		}
		if skipFn != nil && skipFn(ch) {
			continue
		}
		out.AddChild(datach)
	}
	return out
}

func (n *node) Merge() *data.Node {
	return n.merge((Node).Merge, nil)
}

func (n *node) MergeWithoutDefaults() *data.Node {
	return n.merge(
		(Node).MergeWithoutDefaults,
		func(n Node) bool {
			return n.def()
		},
	)
}

//hasOverlay is only needed for testing
func (n *node) hasOverlay() bool {
	return n.overlay != nil
}

//hasUnderlay is only needed for testing
func (n *node) hasUnderlay() bool {
	return n.underlay != nil
}

func (n *node) setIndex(idx uint64) {
	n.idx = idx
}

func (n *node) index() uint64 {
	return n.idx
}

func (n *node) GetSchema() schema.Node {
	return n.schema
}

func (n *node) GetStateJson(path []string) ([][]byte, error) {
	return n.schema.GetStateJson(path)
}

func (n *node) GetStateJsonWithWarnings(
	path []string,
	logger schema.StateLogger,
) ([][]byte, []error) {
	return n.schema.GetStateJsonWithWarnings(path, logger)
}

func (n *node) Data() *data.Node {
	switch {
	case n.overlay != nil:
		return n.overlay
	case n.underlay != nil:
		return n.underlay
	default:
		return nil
	}
}

func (n *node) Name() string {
	d := n.Data()
	if d == nil {
		return ""
	}
	return d.Name()
}

func (n *node) Module() string {
	return n.GetSchema().Module()
}

func (n *node) buildUnionChild(name string) Node {
	sch := n.schema.SchemaChild(name)
	over := n.overlay.Child(name)
	under := n.underlay.Child(name)
	if over != nil && over.Opaque() {
		under = nil
	}
	//If the parent is opaque and there is no overlay for this child
	//then the parent has been recreated to instanciate defaults
	//and we need to ignore the underlay for this child.
	if over == nil && n.opaque() {
		under = nil
	}
	switch {
	case sch == nil:
		//handle schema reload gracefully
		//if we don't have a schema for this child it
		//doesn't exist in the schema tree, bail.
		return nil
	case over != nil && over.Deleted():
		//if the child is deleted then it could have a default value
		return n.buildOpaqueDefaultChild(name, over, under, sch)
	case over == nil && under == nil:
		//if both over and under don't exist then it could
		//be a default whose status depends
		//on the status of 'n'
		return n.buildDefaultChild(name)
	default:
		return NewNode(over, under, sch, n.specialized, getFlags(over))
	}
}

func (n *node) buildOpaqueDefaultChild(name string,
	over, under *data.Node,
	overSchema schema.Node) Node {
	ch := n.schema.DefaultChild(name)
	chcs := n.schema.Choices()

	switch {
	case ch == nil:
		//no default child, just return the deleted child representation
		return NewNode(over, under, overSchema, n.specialized, getFlags(over))
	case len(chcs) > 0:
		//no default child, just return the deleted child representation
		return NewNode(over, under, overSchema, n.specialized, getFlags(over))
	default:
		sch := ch.(schema.Node)
		//otherwise return a default with both default, and deleted flags set.
		//allows copyup to do the right thing.
		dnode := data.New(name)
		dnode.MarkOpaque()
		return NewNode(dnode,
			n.underlay.Child(name),
			sch, n.specialized,
			flagDefault|getFlags(dnode))
	}
}

func isActiveDefaultCase(n *node, nd yang.Node, name, defCase string) bool {
	for _, cd := range nd.Choices() {
		ch := cd.Child(name)
		if ch == nil {
			continue
		}
		if defCase == "" {
			hcfg := hasCfg(n, cd)
			if !hcfg {
				return false
			}
			return isActiveDefault(n, cd, name, false)
		} else if defCase != cd.Name() {
			return false
		}

		for _, cd := range nd.Choices() {
			hcfg := hasCfg(n, cd)
			if hcfg {
				return false
			}
		}
		return isActiveDefault(n, cd, name, cd.Name() == defCase)
	}
	return false
}

func hasCfg(n *node, ch yang.Node) bool {
	och := n.overlay.Children()
	uch := n.underlay.Children()
	for _, v := range och {
		if !v.Deleted() && ch.Child(v.Name()) != nil {
			return true
		}
	}
	for _, v := range uch {
		if !v.Deleted() && ch.Child(v.Name()) != nil {
			nd := n.Child(v.Name())
			if nd != nil && nd.deleted() {
				continue
			}
			return true
		}
	}
	return false
}

func isActiveDefault(n *node, nd yang.Node, name string, defCase bool) bool {
	for _, cd := range nd.Choices() {
		switch choice := cd.(type) {
		case schema.Choice:
			if cd.Child(name) != nil {
				cfg := hasCfg(n, cd)
				switch {
				case cfg == true:
					return isActiveDefaultCase(n, cd, name, "")
				case cd.HasDefault() == true:
					return isActiveDefaultCase(n, cd, name, choice.DefaultCase())
				default:
				}
			}
		default:
			if cd.Name() != name {

			} else {
				if defCase {
					return true
				}
				for _, v := range n.overlay.Children() {
					if nd.Child(v.Name()) != nil {
						return !v.Deleted()
					}
				}
				for _, v := range n.underlay.Children() {
					if !v.Deleted() && nd.Child(v.Name()) != nil {
						return !v.Deleted()
					}
				}
			}
		}
	}
	return false
}

func isAChoice(n *node, name string) bool {
	for _, cd := range n.schema.Choices() {
		switch cd.(type) {
		case schema.Choice, schema.Case:
			if cd.Child(name) != nil {
				return true
			}
		default:
			if cd.Name() == name {
				return true
			}
		}
	}
	return false
}

func (n *node) buildDefaultChild(name string) Node {
	ch := n.schema.DefaultChild(name)
	def := isActiveDefault(n, n.schema, name, false)
	switch {
	case ch == nil:
		return nil

	case isAChoice(n, name) && !def:
		return nil
	default:
		sch := ch.(schema.Node)
		dnode := data.New(name)
		switch {
		case n.Added():
			//If the parent is added then this child is not in the running tree
			return NewNode(dnode, nil, sch, n.specialized, flagDefault)
		case n.deleted():
			//if the parent is deleted then the value could have changed
			//track that in the union node
			dnode.MarkOpaque()
			return NewNode(dnode,
				n.underlay.Child(name),
				sch,
				n.specialized,
				flagDefault|getFlags(dnode))
		default:
			//the default exists in both running and candidate trees
			//make it appear as such
			return NewNode(dnode, dnode, sch, n.specialized, flagDefault)
		}
	}
}

func (n *node) traverseChildren(
	children []string,
	walkFn func(Node),
	skipFn func(Node) bool,
	buildFn func(string) Node,
) {
	for _, child := range children {
		ch := buildFn(child)
		if ch == nil {
			continue
		}
		if skipFn(ch) {
			continue
		}
		walkFn(ch)
	}
}

func (n *node) traverseUnionChildrenSkip(fn func(Node), skip func(Node) bool) {
	seen := make(map[string]struct{})
	traversalFn := func(n Node) {
		seen[n.Name()] = struct{}{}
		fn(n)
	}
	skipFn := func(n Node) bool {
		_, ok := seen[n.Name()]
		if ok {
			return ok
		}
		return skip != nil && skip(n)
	}
	if !n.opaque() {
		//traverse all children in the underlay
		n.traverseChildren(
			n.sortDataChildNames(n.underlay.Children()),
			traversalFn,
			skipFn,
			n.buildUnionChild)
	}
	//traverse all children from the overlay that we haven't yet seen
	n.traverseChildren(
		n.sortDataChildNames(n.overlay.Children()),
		traversalFn,
		skipFn,
		n.buildUnionChild)
	//gross, need to figure out how to remove this special case...
	//leaf nodes can only have one value so, we can't append the
	//defaults.
	if _, leaf := n.specialized.(*Leaf); leaf && !n.emptyNonDefault() {
		return
	}
	//traverse all default children that are not in the overlay or underlay
	//no need to sort; all cases with defaults are order by system.
	n.traverseChildren(
		n.schema.DefaultChildNames(),
		traversalFn,
		skipFn,
		n.buildDefaultChild)
}

func (n *node) traverseUnionChildren(fn func(Node)) {
	n.traverseUnionChildrenSkip(fn, nil)
}

func (n *node) Children() map[string]Node {
	m := make(map[string]Node)
	var idx uint64
	n.traverseUnionChildrenSkip(
		func(n Node) {
			n.setIndex(idx)
			m[n.Name()] = n
			idx++
		},
		func(n Node) bool {
			return n.deleted()
		},
	)
	return m
}

func (n *node) sortDataChildren(children []*data.Node) []*data.Node {
	switch n.schema.OrdBy() {
	case "user":
		sort.Sort(data.ByUser(children))

	case "system":
		sort.Sort(data.BySystem(children))
	}
	return children
}

func (n *node) sortDataChildNames(children []*data.Node) []string {
	children = n.sortDataChildren(children)
	out := make([]string, 0, len(children))
	for _, ch := range children {
		out = append(out, ch.Name())
	}
	return out
}

func (n *node) SortedChildren() []Node {
	chmap := n.Children()
	chs := make([]Node, 0, len(chmap))
	for _, ch := range chmap {
		chs = append(chs, ch)
	}
	switch n.schema.OrdBy() {
	case "user":
		sort.Sort(ByUser(chs))
	case "system":
		sort.Sort(BySystem(chs))
	}
	return chs
}

func (n *node) YangDataName() string {
	return n.Name()
}

func (n *node) YangDataChildren() []datanode.DataNode {
	return n.yangDataChildren(true)
}

func (n *node) YangDataChildrenNoSorting() []datanode.DataNode {
	return n.yangDataChildren(false)
}

func (n *node) yangDataChildren(sortChildren bool) []datanode.DataNode {
	y_chs := make([]datanode.DataNode, 0, 0)

	if sch, ok := n.schema.(schema.ListEntry); ok {
		name := sch.Keys()[0]
		new_node := datanode.CreateDataNode(name, nil, []string{n.Name()})
		y_chs = append(y_chs, new_node)
	}
	if sortChildren {
		for _, ch := range n.SortedChildren() {
			if !ch.deleted() {
				y_chs = append(y_chs, ch)
			}
		}
	} else {
		for _, ch := range n.Children() {
			if !ch.deleted() {
				y_chs = append(y_chs, ch)
			}
		}
	}
	return y_chs
}

func (n *node) YangDataValues() []string {
	return n.yangDataValues(true)
}

func (n *node) YangDataValuesNoSorting() []string {
	return n.yangDataValues(false)
}

func (n *node) yangDataValues(sortChildren bool) []string {
	y_chs := make([]string, 0, 0)
	if sortChildren {
		for _, ch := range n.SortedChildren() {
			if !ch.deleted() {
				y_chs = append(y_chs, ch.Name())
			}
		}
	} else {
		for _, ch := range n.Children() {
			if !ch.deleted() {
				y_chs = append(y_chs, ch.Name())
			}
		}
	}
	return y_chs
}

func (n *node) NumChildren() int {
	var count int
	n.traverseUnionChildrenSkip(
		func(_ Node) {
			count++
		},
		func(n Node) bool {
			return n.deleted()
		},
	)
	return count
}

func (n *node) opaque() bool {
	if n.overlay == nil {
		return false
	}
	return n.overlay.Opaque()
}

func (n *node) deleted() bool {
	if n.overlay == nil {
		return false
	}
	return n.overlay.Deleted()
}

func (n *node) def() bool {
	return n.flags&flagDefault == flagDefault
}

func (n *node) Child(name string) Node {
	return n.buildUnionChild(name)
}

func (n *node) Parent() Node {
	return n.parent
}

func (n *node) addChild(child *data.Node) Node {
	name := child.Name()
	//CopyUp self
	parent := n.copyUp()
	//find child, overlay will always exist after CopyUp
	overlay := parent.Data()
	ch := overlay.Child(name)
	switch {
	case ch == nil:
		overlay.AddChild(child)
	case ch.Deleted():
		//this is subtle, in order for indicies to be
		//correct for user order, we cannot reuse the found child
		//we must add a new opaque child
		child.MarkOpaque()
		overlay.AddChild(child)
	}
	return parent.Child(name)
}

func (n *node) Added() bool {
	return n.overlay != nil && n.underlay == nil
}

func (n *node) Updated() bool {
	for _, ch := range n.Children() {
		if ch.Changed() {
			return true
		}
	}
	return false
}

func (n *node) Changed() bool {
	return n.Added() || n.Updated() || n.deleted()
}

func (n *node) Empty() bool {
	return n.NumChildren() == 0
}

func (n *node) copyUp() Node {
	if n.overlay != nil && !n.def() {
		//If the overlay node exists but is deleted
		//we need to mark as not deleted, but still opaque
		//The lower layer has a mechanism for doing this.
		if n.deleted() {
			n.overlay.ClearDeleted()
		}
		//already in the upperlayer
		//the roots will always have an overlay so
		//recursion will terminate here.
		return n.specialized
	}
	//Copy the underlay data node and then add it to the parent's overlay
	//we know the parent has an overlay because of the CopyUp operation
	newNode := n.parent.addChild(n.Data().Copy()).getnode()
	n.overlay = newNode.overlay
	n.underlay = newNode.underlay
	n.schema = newNode.schema
	n.parent = newNode.parent
	n.flags = newNode.flags
	return n.specialized
}

type walker func(Node, string, []string) error
type laster func(Node) error

//walkPath is a generic algorithm for walking the configuration path to
//preform actions. The actions are abstracted in the chExists, chNotExists
//and last functions. chExists is called when a child node is found. chNotExists
//is called in the other case. last is called at the end of the path and
//signifies the end of recursion down the path
func (n *node) walkPath(
	chExists walker,
	chNotExists walker,
	last laster,
	path []string,
) error {
	if len(path) == 0 {
		return last(n.specialized)
	}
	hd, tl := path[0], path[1:]
	ch := n.Child(hd)
	if ch == nil || (ch.deleted() && !ch.def()) {
		return chNotExists(ch, hd, tl)
	}
	return chExists(ch, hd, tl)
}

//TODO: Collapse notExists and exists
func (n *node) notExists(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return yang.NewNodeExistsError(append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			return ch.notExists(tl, append(curPath, hd))
		},
		func(_ Node) error {
			return nil
		},
		path,
	)
}

func (n *node) exists(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return ch.exists(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			return yang.NewNodeNotExistsError(append(curPath, hd))
		},
		func(n Node) error {
			if n == nil {
				return yang.NewNodeNotExistsError(curPath)
			}
			return nil
		},
		path,
	)
}

func (n *node) setHook() {}

func choiceNodesToRemove(chcs []yang.Node, name string, rslt []string) []string {

	for _, choice := range chcs {
		switch choice.(type) {
		case schema.Choice:
			if choice.Child(name) != nil {
				rslt = caseNodesToRemove(choice.Choices(), name, rslt)
			}
		}
	}

	return rslt
}
func caseNodesToRemove(chcs []yang.Node, name string, rslt []string) []string {

	for _, choice := range chcs {
		switch choice.(type) {

		case schema.Case:
			if choice.Child(name) != nil {
				rslt = choiceNodesToRemove(choice.Choices(), name, rslt)
			} else {
				for _, ch := range choice.Children() {
					rslt = append(rslt, ch.Name())
				}
			}

		default:
			if choice.Name() != name {
				rslt = append(rslt, choice.Name())
			}
		}
	}

	return rslt
}

func (n *node) set(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			ch = ch.copyUp()
			ch.setHook()
			return ch.set(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			//if ch wasn't found (this case) then the
			//passed in child is nil, so we need to do
			//a lookup after adding the new child
			rslt := make([]string, 0)
			if n.schema.Choices() != nil {
				rslt = choiceNodesToRemove(n.schema.Choices(), hd, rslt)
			}
			if n.schema.Status() != yang.Obsolete {
				ch = n.addChild(data.New(hd))
				ch.setHook()
				if err := ch.set(tl, append(curPath, hd)); err != nil {
					return err
				}
			}
			for _, rn := range rslt {
				chn := n.Child(rn)
				if chn != nil {
					err := callInternalWalker(chn.deleteEverythingUnder, append(curPath, rn))
					if err != nil {
						return err
					}
				}
			}
			return nil
		},
		func(_ Node) error {
			return nil
		},
		path,
	)
}

func (n *node) emptyNonDefault() bool {
	//This checks for an empty node that doesn't account for defaults.
	//It is also the fast path for empty as it doesn't do any allocations.
	if n.overlay != nil {
		var numdel int

		if n.overlay.Deleted() {
			return true
		}
		for _, over := range n.overlay.ChildMap() {
			if over.Deleted() {
				numdel++
				continue
			}
			return false
		}
		if n.underlay == nil && numdel == n.overlay.NumChildren() {
			return true
		}
		if n.opaque() {
			return true
		}
	}
	if n.underlay != nil {
		var numdel int
		for _, under := range n.underlay.ChildMap() {
			name := under.Name()
			if over := n.overlay.Child(name); over != nil && over.Deleted() {
				numdel++
				continue
			}
			return false
		}
		if numdel == n.underlay.NumChildren() {
			return true
		}
	}
	return true
}

// For the likes of load operations where we delete multiple nodes then add
// some back, we need to ensure we don't clear the flags on child nodes when
// deleting parent nodes or we will end up recreating deleted nodes if we
// recreate the parent.  Boolean is passed in to set this behaviour.
func (n *node) markDeleted(clearChildFlagsWhenDeletingParent bool) {
	//copyUp().Data() is the nasty way to
	//always get an overlay child, probably
	//should find a cleaner way to do this
	n.copyUp().Data().MarkDeleted(clearChildFlagsWhenDeletingParent)
}

// Similarly to markDeleted, behaviour is different if we need to check
// authorization on each node before deleting, rather than just propagating
// the top-level permission.
//
// The presence check only applies to the checkAuth=false mode of operation as
// in such cases we do not wish to delete parent presence nodes which have no
// remaining children.  When checkAuth=true, we are deleting everything we are
// allowed to delete, including presence nodes.
//
func (n *node) deleteIfEmpty(checkAuth bool) {
	if (checkAuth || !n.schema.HasPresence()) && n.emptyNonDefault() {
		flag := data.ClearChildFlags
		if checkAuth {
			flag = data.DontClearChildFlags
		}
		n.markDeleted(flag)
		n.deleteEmptyParent(checkAuth)
	}
}

func (n *node) deleteEmptyParent(checkAuth bool) {
	//recursive walk up the tree to
	//remove empty parent nodes when
	//a child has been deleted
	if n.parent == nil {
		return
	}
	n.parent.deleteIfEmpty(checkAuth)
}

// 'delete' can operate in 2 modes.  Either we nuke everything under
// the given path, or we carefully check authorization on each node
// starting at the bottom (leaf nodes).
//
// - deleteEverythingUnder() provides the first option
// - deleteCheckAuth() does the bottom up with auth checking
//
// <path> is the path to the current node
// <curPath> is the path to where we want to delete, starting at current node.
//
func (n *node) deleteEverythingUnder(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return ch.deleteEverythingUnder(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			ch = n.copyUp()
			return ch.deleteEverythingUnder(tl, append(curPath, hd))
		},
		func(last Node) error {
			last.markDeleted(data.ClearChildFlags)
			last.deleteEmptyParent(DontCheckAuth)
			return nil
		},
		path,
	)
}

func (n *node) deleteCheckAuth(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return ch.deleteCheckAuth(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			ch = n.copyUp()
			return ch.deleteCheckAuth(tl, append(curPath, hd))
		},
		func(last Node) error {
			last.markDeleted(data.DontClearChildFlags)
			last.deleteEmptyParent(CheckAuth)
			return nil
		},
		path,
	)
}

func (n *node) get(path, curPath []string) ([]string, error) {
	//Predeclare output so closures below can populate it.
	var out []string
	err := n.walkPath(
		func(ch Node, hd string, tl []string) error {
			var err error
			out, err = ch.get(tl, append(curPath, hd))
			return err
		},
		func(ch Node, hd string, tl []string) error {
			//get always returns a slice, never an error
			//this preserves the previous behavior
			//perhaps this should be rethought and
			//return NewNodeNotExistsError(append(curPath, hd)) ?
			out = []string{}
			return nil
		},
		func(last Node) error {
			children := last.SortedChildren()
			out = make([]string, 0, len(children))
			for _, ch := range children {
				out = append(out, ch.Name())
			}
			return nil
		},
		path,
	)
	return out, err
}

func (n *node) descendant(path, curPath []string) (Node, error) {
	var out Node
	err := n.walkPath(
		func(ch Node, hd string, tl []string) error {
			var err error
			out, err = ch.descendant(tl, append(curPath, hd))
			return err
		},
		func(ch Node, hd string, tl []string) error {
			return yang.NewNodeNotExistsError(append(curPath, hd))
		},
		func(last Node) error {
			out = last
			return nil
		},
		path,
	)
	return out, err
}

func (n *node) validateNotExistsSet(path, curPath []string) error {
	// Set checks for an odd case of existance, the value can exist if it
	// is set as the default this allows us to set the value to the default
	// Then it will exist in the tree as a user set value and will appear if
	// the user chooses serialize without defaults.
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return ch.validateNotExistsSet(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			return nil
		},
		func(last Node) error {
			if last.def() {
				return nil
			}
			return yang.NewNodeExistsError(curPath)
		},
		path,
	)
}

func (n *node) validateDeletePath(path, curPath []string) error {
	return n.walkPath(
		func(ch Node, hd string, tl []string) error {
			return ch.validateDeletePath(tl, append(curPath, hd))
		},
		func(ch Node, hd string, tl []string) error {
			return yang.NewNodeNotExistsError(append(curPath, hd))
		},
		func(last Node) error {
			if last.def() {
				return yang.NewNodeNotExistsError(curPath)
			}
			return nil
		},
		path,
	)
}

func (n *node) isDefault(path, curPath []string) (bool, error) {
	var out bool
	err := n.walkPath(
		func(ch Node, hd string, tl []string) error {
			var err error
			out, err = ch.isDefault(tl, append(curPath, hd))
			return err
		},
		func(ch Node, hd string, tl []string) error {
			out = false
			return nil
		},
		func(last Node) error {
			out = last.def()
			return nil
		},
		path,
	)
	return out, err
}

func (n *node) show(path, curPath []string, opts *unionOptions) (string, error) {
	var out string
	err := n.walkPath(
		func(ch Node, hd string, tl []string) error {
			var err error
			out, err = ch.show(tl,
				append(curPath, hd),
				opts)
			return err
		},
		func(ch Node, hd string, tl []string) error {
			return yang.NewNodeNotExistsError(append(curPath, hd))
		},
		func(last Node) error {
			var b StringWriter
			//Do we need to pass flags to show or do we treat it as
			//a shortcut to serialize and make it hide secrets and
			//defaults.
			last.serialize(&b, curPath, 0, opts)
			out = b.String()
			return nil
		},
		path,
	)
	return out, err
}

func (n *node) serialize(
	b Serializer,
	cpath []string,
	lvl int,
	opts *unionOptions,
) {
	n.specialized.serialize(b, cpath, lvl, opts)
}

func (n *node) Serialize(b Serializer, path []string, options ...UnionOption) {
	var opts unionOptions
	for _, opt := range options {
		opt(&opts)
	}

	// If this is not the root node and no path was provided then set
	// the path to the name of the current node. This ensures that a complete
	// path is provided to the auth layer for authorization and accounting.
	if n.parent != nil && path == nil {
		path = []string{n.Name()}
	}

	n.specialized.serialize(b, path, 0, &opts)
}

func callInternalWalker(fn func([]string, []string) error, path []string) error {
	return fn(path, make([]string, 0, len(path)))
}

func (n *node) IsDefault(auth Auther, path []string) (bool, error) {
	if !authorize(auth, path, "read") {
		return false, autherr
	}
	return n.isDefault(path, make([]string, 0, len(path)))
}

func (n *node) Set(auth Auther, path []string) error {
	//Not sure that this should be here, but it
	//makes the implementation easier for now.
	ctx := schema.ValidateCtx{
		Path:    pathutil.Pathstr(path),
		CurPath: path,
		Noexec:  true,
	}
	err := n.schema.Validate(ctx, []string{}, path)
	if err != nil {
		return err
	}

	//TODO: If is secret, and user isn't secrets group, silently ignrore
	// set exists error
	if len(path) == 0 {
		return yang.NewNodeExistsError([]string{})
	}

	sn := schema.Descendant(n.schema, path)
	if sn.Status() == yang.Obsolete {
		// Silently drop
		return nil
	}

	err = callInternalWalker(n.validateNotExistsSet, path)
	if err != nil {
		sch := schema.Descendant(n.schema, path)
		if sch != nil && sch.ConfigdExt().Secret &&
			!authorize(auth, path, "secrets") {
			return nil
		}
		return err
	}
	//TODO: create? currently this is broken in configd anyway,
	//      replicating behavior...
	//      how do we fix it?
	if !authorize(auth, path, "update") {
		return autherr
	}
	return callInternalWalker(n.set, path)
}

// Two modes of operation:
//
// - CheckAuth:     check authorization on each node.  Bottom up deletion
// - DontCheckAuth: propagate top level authorization.  Top down deletion
//
func (n *node) Delete(auth Auther, path []string, checkAuth bool) error {
	if checkAuth {
		if n.Name() != "root" {
			err := mgmterror.NewOperationFailedApplicationError()
			err.Message = "Delete operation must be run from root node."
			return err
		}

		return n.deleteWalkerCheckAuth(auth, n, []string{})
	}

	if len(path) == 0 {
		return yang.NewNodeNotExistsError(path)
	}
	err := callInternalWalker(n.validateDeletePath, path)
	if err != nil {
		return err
	}
	if !authorize(auth, path, "delete") {
		return autherr
	}
	return callInternalWalker(n.deleteEverythingUnder, path)
}

func (n *node) deleteWalkerCheckAuth(
	auth Auther,
	root Node,
	path []string,
) error {
	var childPath []string
	for _, ch := range n.Children() {
		childPath = pathutil.CopyAppend(path, ch.Name())
		ch.deleteWalkerCheckAuth(auth, root, childPath)
	}
	if n.NumChildren() == 0 {
		if authorize(auth, path, "delete") {
			return n.deleteCheckAuth(path, []string{})
		}
	}

	return nil
}

func (n *node) Exists(auth Auther, path []string) error {
	if !authorize(auth, path, "read") {
		return autherr
	}
	return callInternalWalker(n.exists, path)
}

func (n *node) Descendant(auth Auther, path []string) (Node, error) {
	if !authorize(auth, path, "read") {
		return nil, autherr
	}
	return n.descendant(path, make([]string, 0, len(path)))
}

func (n *node) Get(auth Auther, path []string) ([]string, error) {
	if !authorize(auth, path, "read") {
		return nil, autherr
	}
	ret, err := n.get(path, make([]string, 0, len(path)))
	if auth == nil {
		//fast path for no authorization
		return ret, err
	}

	out := make([]string, 0, len(ret))
	hide := false

	// The only secret values which can be hidden are leaf(-list) values.
	// This allows this check to be performed only once, applying result to
	// descendant path elements.
	sch := schema.Descendant(n.schema, path)

	if sch != nil {
		secret := sch.ConfigdExt().Secret

		switch v := sch.(type) {
		case schema.List:
			// For a list node, we need to get secret status from the keynode.
			// To get the keynode, we need find the lists child ListEntry, and
			// from there get the keynode.
			lentry := sch.Child(v.Keys()[0])
			if lentry != nil {
				keynode := lentry.Child(v.Keys()[0])
				secret = keynode.(schema.ExtendedNode).ConfigdExt().Secret
			}
		}

		if secret {
			if !authorize(auth, path, "secrets") {
				hide = true
			}
		}
	}
	for _, val := range ret {
		if !authorize(auth, pathutil.CopyAppend(path, val), "read") {
			continue
		}

		switch {
		case hide == true:
			out = append(out, quote("********"))
		default:
			out = append(out, val)
		}
	}

	return out, err
}

func (n *node) Show(path []string, options ...UnionOption) (string, error) {
	var opts unionOptions
	for _, opt := range options {
		opt(&opts)
	}

	if !authorize(opts.auth, path, "read") {
		return "", autherr
	}
	return n.show(path, make([]string, 0, len(path)), &opts)
}

func (n *node) Default() bool {
	return n.def()
}

func NewNode(overlay, underlay *data.Node, sch schema.Node, parent Node, flags Flags) Node {
	switch v := sch.(type) {
	case schema.Container:
		return NewContainer(overlay, underlay, v, parent, flags)
	case schema.List:
		return NewList(overlay, underlay, v, parent, flags)
	case schema.ListEntry:
		return NewListEntry(overlay, underlay, v, parent, flags)
	case schema.Leaf:
		return NewLeaf(overlay, underlay, v, parent, flags)
	case schema.LeafList:
		return NewLeafList(overlay, underlay, v, parent, flags)
	case schema.Choice:
		return NewChoice(overlay, underlay, v, parent, flags)
	case schema.LeafValue:
		return NewLeafValue(overlay, underlay, v, parent, flags)
	case schema.Tree:
		return NewRoot(overlay, underlay, v, parent, flags)
	}
	return nil
}

type Root struct {
	*node
	Schema schema.Tree
}

func NewRoot(overlay, underlay *data.Node, sch schema.Tree, parent Node, flags Flags) *Root {
	out := new(Root)
	out.node = newNode(overlay, underlay, sch, parent, out, flags)
	out.Schema = sch
	return out
}

func (n *Root) serialize(b Serializer, path []string, lvl int, opts *unionOptions) {
	n.serializeChildren(b, path, 0, opts)
}

func getFlags(overlay *data.Node) Flags {
	var out Flags
	if overlay == nil {
		return out
	}
	if overlay.Deleted() {
		out = out | flagDeleted
	}
	if overlay.Opaque() {
		out = out | flagOpaque
	}
	return out
}
