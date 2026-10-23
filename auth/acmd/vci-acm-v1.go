//Copyright (c) 2018-2019, AT&T Intellectual Property.
// All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package acmd

import (
	"github.com/danos/config/auth"
	"github.com/danos/encoding/rfc7951"
)

type AcmV1Config struct {
	Enable              bool   `rfc7951:"vyatta-system-acm-configd-v1:enable,emptyleaf" json:"enabled"`
	LogReq              bool   `rfc7951:"vyatta-system-acm-configd-v1:log-requests,emptyleaf" json:"log-requests"`
	CreateDefault       Action `rfc7951:"vyatta-system-acm-configd-v1:create-default,omitempty" json:"create-default"`
	ReadDefault         Action `rfc7951:"vyatta-system-acm-configd-v1:read-default,omitempty" json:"read-default"`
	UpdateDefault       Action `rfc7951:"vyatta-system-acm-configd-v1:update-default,omitempty" json:"update-default"`
	DeleteDefault       Action `rfc7951:"vyatta-system-acm-configd-v1:delete-default,omitempty" json:"delete-default"`
	RpcDefault          Action `rfc7951:"vyatta-system-acm-configd-v1:rpc-default,omitempty" json:"rpc-default"`
	NotificationDefault Action `rfc7951:"vyatta-system-acm-configd-v1:notification-default,omitempty" json:"notification-default"`
	// The rulesets are presence containers: a pointer keeps "configured but
	// without rules" distinct from "not configured" (A02 stage 3-F, F6).
	Ruleset             *AcmV1Ruleset             `rfc7951:"vyatta-system-acm-configd-v1:ruleset,omitempty" json:"-"`
	RpcRuleset          *AcmV1RpcRuleset          `rfc7951:"vyatta-system-acm-configd-v1:rpc-ruleset,omitempty" json:"-"`
	NotificationRuleset *AcmV1NotificationRuleset `rfc7951:"vyatta-system-acm-configd-v1:notification-ruleset,omitempty" json:"-"`
}

type AcmV1Ruleset struct {
	Rule []*AcmV1Rule `rfc7951:"rule,omitempty"`
}
type AcmV1RpcRuleset struct {
	Rule []*AcmV1RpcRule `rfc7951:"rule,omitempty"`
}
type AcmV1NotificationRuleset struct {
	Rule []*AcmV1NotificationRule `rfc7951:"rule,omitempty"`
}

func (a *AcmV1Config) rules() []*AcmV1Rule {
	if a.Ruleset == nil {
		return nil
	}
	return a.Ruleset.Rule
}

func (a *AcmV1Config) rpcRules() []*AcmV1RpcRule {
	if a.RpcRuleset == nil {
		return nil
	}
	return a.RpcRuleset.Rule
}

func (a *AcmV1Config) notificationRules() []*AcmV1NotificationRule {
	if a.NotificationRuleset == nil {
		return nil
	}
	return a.NotificationRuleset.Rule
}

type AcmV1Rule struct {
	RuleNumber uint32   `rfc7951:"tagnode" json:"-"`
	Path       *string  `rfc7951:"path" json:"path"`
	Action     Action   `rfc7951:"action,omitempty" json:"action"`
	Groups     []string `rfc7951:"group,omitempty" json:"groups"`
	Log        bool     `rfc7951:"log,emptyleaf" json:"log"`
	Operation  AcmPerm  `rfc7951:"operation,omitempty" json:"perm"`
}
type AcmV1RpcRule struct {
	RuleNumber uint32   `rfc7951:"rule-number" json:"-"`
	Rpc        *string  `rfc7951:"rpc-name" json:"rpc-name"`
	Module     string   `rfc7951:"module-name,omitempty"`
	Action     Action   `rfc7951:"action,omitempty" json:"action"`
	Groups     []string `rfc7951:"group,omitempty" json:"groups"`
}
type AcmV1NotificationRule struct {
	RuleNumber   uint32   `rfc7951:"rule-number" json:"-"`
	Notification *string  `rfc7951:"notification-name" json:"notification-name"`
	Module       string   `rfc7951:"module-name,omitempty"`
	Action       Action   `rfc7951:"action,omitempty" json:"action"`
	Groups       []string `rfc7951:"group,omitempty" json:"groups"`
}

// Translate the ruleset and rpc-ruleset rules into an Authdb
// suitable for saving to configruleset file
// Even though RPC rules are enforced by D-Bus, we still
// require them for non VCI clients.
func (a *AcmV1Config) translateToConfigRuleSet(crs *auth.Authdb) {
	crs.Enabled = a.Enable
	crs.LogReq = a.LogReq
	crs.CreateDefault = a.CreateDefault.translateToConfigRules()
	crs.ReadDefault = a.ReadDefault.translateToConfigRules()
	crs.UpdateDefault = a.UpdateDefault.translateToConfigRules()
	crs.DeleteDefault = a.DeleteDefault.translateToConfigRules()
	crs.RpcDefault = a.RpcDefault.translateToConfigRules()
	crs.ExecDefault = auth.AUTH_ALLOW

	// Translate the data node rules
	if len(a.rules()) > 0 {
		rules := make([]*auth.AuthRuleType, 0)
		for _, r := range a.rules() {
			var authtype auth.AuthType
			rule := &auth.AuthRule{}
			rule.Path = *r.Path
			authtype = auth.AUTH_T_DATA
			rule.Perm = r.Operation.translateToConfigRules()
			rule.Action = r.Action.translateToConfigRules()
			rule.Groups = append(rule.Groups, r.Groups...)
			ruletype := &auth.AuthRuleType{Type: authtype, Rule: rule}
			rules = append(rules, ruletype)
		}
		crs.Rules = rules
	}

	// Translate the RPC rules
	if len(a.rpcRules()) > 0 {
		rules := make([]*auth.AuthRuleType, 0)
		for _, r := range a.rpcRules() {
			var authtype auth.AuthType
			rule := &auth.AuthRule{}
			if r.Rpc != nil {
				rule.Rpc = *r.Rpc
			}
			authtype = auth.AUTH_T_RPC
			rule.Module = r.Module
			rule.Action = r.Action.translateToConfigRules()
			rule.Groups = append(rule.Groups, r.Groups...)
			ruletype := &auth.AuthRuleType{Type: authtype, Rule: rule}
			rules = append(rules, ruletype)
		}
		crs.RpcRules = rules
	}
}

// MarshalRFC7951 returns the configuration in the shape configd sends it
// (A02 stage 3-F, F6): only what was configured. An absent acm container is
// not written as an empty one, and the struct's own tags (omitempty on every
// optional leaf, pointer presence containers) keep unconfigured leaves and
// rulesets out while a configured-but-empty ruleset stays as {}.
func (c *AcmdConfig) MarshalRFC7951() ([]byte, error) {
	if c.System.Acm.AcmV1Config == nil {
		return []byte("{}"), nil
	}
	return rfc7951.Marshal((*plainAcmdConfig)(c))
}

// plainAcmdConfig has AcmdConfig's fields but not its MarshalRFC7951 method.
type plainAcmdConfig AcmdConfig
