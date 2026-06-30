package impulsering

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// killBroker SIGKILLs the broker (no graceful unlink) and reaps it so its pid is
// free before the replacement starts. Models an abrupt impulsed crash/restart.
func killBroker(cmd *exec.Cmd) {
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func callAdd(c *Connection, a, b int64) (int64, error) {
	e := NewEncoder()
	e.PutLong(a)
	e.PutLong(b)
	resp, err := c.Call("add", "fn-key", e.Bytes(), 3000)
	if err != nil {
		return 0, err
	}
	return NewDecoder(resp).Long(), nil
}

// TestBrokerRestartRecovery asserts that an existing service + client survive a
// broker restart: the function is re-exposed and the client's RPC keeps working
// without rebuilding any handle.
func TestBrokerRestartRecovery(t *testing.T) {
	broker := startBroker(t)
	killed := false
	defer func() {
		if !killed {
			broker.Process.Signal(os.Interrupt)
			broker.Wait()
		}
	}()

	svc, err := Connect("go-svc-restart")
	if err != nil {
		t.Fatalf("connect svc: %v", err)
	}
	defer svc.Close()
	if err := svc.ExposeFunction("add", addReqSchema, addRespSchema, "fn-key", addHandler); err != nil {
		t.Fatalf("expose: %v", err)
	}

	cli, err := Connect("go-cli-restart")
	if err != nil {
		t.Fatalf("connect cli: %v", err)
	}
	defer cli.Close()
	epochBefore := cli.BrokerEpoch()

	if sum, err := callAdd(cli, 7, 35); err != nil || sum != 42 {
		t.Fatalf("rpc before restart: sum=%d err=%v", sum, err)
	}

	// Restart the broker (new shared-memory generation + epoch).
	killBroker(broker)
	killed = true
	broker2 := startBroker(t)
	defer func() {
		broker2.Process.Signal(os.Interrupt)
		broker2.Wait()
	}()

	// Retry until the connection reconnects and the function is re-exposed.
	ok := false
	for i := 0; i < 100; i++ {
		if sum, err := callAdd(cli, 20, 22); err == nil && sum == 42 {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatal("client never recovered after restart")
	}
	if cli.BrokerEpoch() == epochBefore {
		t.Fatal("epoch did not advance after restart")
	}
}
