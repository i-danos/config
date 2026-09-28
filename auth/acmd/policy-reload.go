// Copyright (c) 2018-2019, AT&T Intellectual Property.
// All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package acmd

import (
	"github.com/danos/godbus/dbus"
	"github.com/danos/objtree"
)

type dBusConnector func(dbus.Handler, dbus.SignalHandler) (*dbus.Conn, error)

type dbusTransport struct {
	busMgr    *objtree.BusManager
	conn      *dbus.Conn
	connectFn dBusConnector
}

func (t *dbusTransport) connectVciFn(
	hdlr dbus.Handler,
	_ dbus.SignalHandler,
) (*dbus.Conn, error) {
	return dbus.Dial("unix:path=/var/run/vci/vci_bus_socket")
}

func newVciTransport() *dbusTransport {
	t := &dbusTransport{}
	t.connectFn = dBusConnector(t.connectVciFn)
	return t

}

func (t *dbusTransport) Dial() error {
	busMgr, err := objtree.NewAnonymousBusManager(t.connectFn)
	if err != nil {
		return err
	}
	t.busMgr = busMgr
	t.conn = busMgr.Conn()
	return nil
}

func (t *dbusTransport) Close() error {
	return t.conn.Close()
}

func (t *dbusTransport) policyReload() error {
	call := t.conn.BusObject().Call("org.freedesktop.DBus.ReloadConfig", 0)
	if call.Err != nil {
		return call.Err
	}
	return nil
}
func reloadPolicyRules() error {
	t := newVciTransport()
	err := t.Dial()
	if err != nil {
		return err
	}
	defer t.Close()
	err = t.policyReload()
	if err != nil {
	}

	return err
}
