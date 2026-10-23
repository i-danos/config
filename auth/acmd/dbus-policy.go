//Copyright (c) 2018-2019, AT&T Intellectual Property.
// All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package acmd

import (
	"bytes"
	"encoding/xml"
	"strings"
	"unicode"
)

const policyHeader = `<!DOCTYPE busconfig PUBLIC
 "-//freedesktop//DTD D-BUS Bus Configuration 1.0//EN"
  "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
`

type RuleAttributes struct {
	RcvType       string `xml:"receive_type,attr,omitempty"`
	RcvInterface  string `xml:"receive_interface,attr,omitempty"`
	RcvMember     string `xml:"receive_member,attr,omitempty"`
	SendType      string `xml:"send_type,attr,omitempty"`
	SendInterface string `xml:"send_interface,attr,omitempty"`
	SendMember    string `xml:"send_member,attr,omitempty"`
}

type Rule struct {
	Action string
	RuleAttributes
}

func newRule() *Rule {
	return &Rule{}
}

func (r *Rule) action(a string) *Rule {
	r.Action = a
	return r
}

func (r *Rule) methodcallType() *Rule {
	r.SendType = "method_call"
	return r
}

func (r *Rule) receiveSignal() *Rule {
	r.RcvType = "signal"
	return r
}

// A D-Bus policy that applies to all notification defined in a module
// Specifying module as "*", the rule will apply to all Notifications
// in all modules
func (r *Rule) allNotifications(module string) *Rule {
	if module == "*" {
		r = r.receiveSignal()
		r.RcvInterface = module
		return r
	}
	iface := bytes.NewBufferString("yang.module.")
	iface.WriteString(convertYangNameToDBus(module))
	iface.WriteString(".Notification")
	r.RcvInterface = iface.String()
	return r
}

// A D-Bus policy rule that applies to a single Notification
func (r *Rule) singleNotification(notification string) *Rule {
	parts := strings.Split(notification, ":")
	if len(parts) < 2 {
		return r
	}
	iface := convertYangNameToDBus(parts[0])
	member := convertYangNameToDBus(parts[1])
	r.RcvInterface = "yang.module." + iface + ".Notification"
	r.RcvMember = member
	return r
}

// This is required to allow a dynamic Action for each rule allow/deny
func (r *Rule) MarshalXML(e *xml.Encoder, start xml.StartElement) error {

	e.EncodeElement(r.RuleAttributes, xml.StartElement{Name: xml.Name{Local: r.Action}})
	return nil
}

// A D-Bus default context attribute

type Policy struct {
	XMLName xml.Name `xml:"policy"`
	Context string   `xml:"context,attr,omitempty"`
	Group   string   `xml:"group,attr,omitempty"`
	Rules   []*Rule  `xml:"-,omitempty"`
}

func newPolicy() *Policy {
	return &Policy{}
}

// A D-Bus policy that is applies the Notification default actions
func (p *Policy) Defaults(notification string) *Policy {
	p.Context = "default"
	rules := make([]*Rule, 0)
	rules = append(rules, newRule().action(notification).receiveSignal())
	p.Rules = rules
	return p
}

func (p *Policy) group(g string) *Policy {
	p.Group = g
	return p
}
func (p *Policy) rules(rules []*Rule) *Policy {
	p.Rules = rules
	return p
}

type Busconfig struct {
	XMLName  xml.Name `xml:"busconfig"`
	Policies []*Policy
}

func newBusconfig(policies []*Policy) *Busconfig {
	return &Busconfig{Policies: policies}
}

// Translate the notification-rulset
// into a set of D-Bus policies rules.
//
// The policy rules need to be in a per GroupId policy ruleset.
// Any rule applying to multiple groups needs duplicated into
// each policy group.
// D-Bus policy rules are evaluated in last-match wins order,
// therefore each policy rule list is built in reverse order.
//
func (a *AcmV1Config) translateToPolicy() ([]byte, error) {
	grps := make(map[string][]*Rule)
	var rl *Rule

	policies := make([]*Policy, 0)

	if !a.Enable {
		// ACM is disabled, Write a policy that allows
		// All Notifications
		policies = append(policies, newPolicy().Defaults("allow"))

		return marshalDbusPolicy(newBusconfig(policies))
	}

	// Translate Notification rules
	for _, r := range a.notificationRules() {
		rl = newRule().action(r.Action.String())
		if r.Notification != nil && *r.Notification != "*" {
			rl = rl.singleNotification(*r.Notification)
		} else {
			rl = rl.allNotifications(r.Module)
		}
		for _, g := range r.Groups {
			if _, ok := grps[g]; ok {
				// Add rule to start of list
				grps[g] = append([]*Rule{rl}, grps[g]...)
			} else {
				rls := make([]*Rule, 0)
				// Add rule to start of list
				rls = append([]*Rule{rl}, rls...)
				grps[g] = rls
			}
		}
	}

	// Create policy rule for notification default actions
	// These should be ordered before other policy rules
	policies = append(policies, newPolicy().Defaults(a.NotificationDefault.String()))

	// Create each policy ruleset, one per GroupId
	for g, rs := range grps {
		pol := newPolicy().group(g).rules(rs)
		policies = append(policies, pol)
	}

	return marshalDbusPolicy(newBusconfig(policies))
}

// Convert a Yang name to a D-Bus name
// Start of Name is uppercase, hyphen is removed and
// following character is swithced to uppercase
// Example: vyatta-op-v1 -> VyattaOpV1
func convertYangNameToDBus(name string) string {
	var afterHyphen bool
	var buf []byte
	b := bytes.NewBuffer(buf)
	for i, r := range name {
		if r == '-' {
			afterHyphen = true
			continue
		} else if i == 0 || afterHyphen {
			b.WriteRune(unicode.ToUpper(r))
			afterHyphen = false
		} else {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// Marshal the D-Bus policy to XML, attaching the required
// D-Bus rules file header
func marshalDbusPolicy(busconfig *Busconfig) ([]byte, error) {

	var out bytes.Buffer
	output, err := xml.MarshalIndent(busconfig, " ", "   ")
	if err != nil {
		return nil, err
	}
	out.WriteString(policyHeader)
	out.Write(output)
	return out.Bytes(), nil
}
