// +build !darwin

package dbus

import (
	"errors"
	"os"
	"os/exec"
	"strings"
)

func getSessionBusPlatformAddress() (string, error) {
	cmd := exec.Command("dbus-launch")
	b, err := cmd.CombinedOutput()

	if err != nil {
		return "", err
	}

	line := ""
	for _, line = range strings.Split(string(b), "\n") {
		if strings.Contains(line, "DBUS_SESSION_BUS_ADDRESS") {
			break
		}
	}

	if line == "" {
		return "", errors.New("dbus: couldn't determine address of session bus")
	}

	i := strings.Index(line, "=")
	if i == -1 {
		return "", errors.New("dbus: couldn't parse address of session bus")
	}

	env, addr := line[:i], line[i+1:]
	os.Setenv(env, addr)

	return addr, nil
}
