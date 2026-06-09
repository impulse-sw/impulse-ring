package impulsering

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	metricSchema  = `{"type":"record","name":"Metric","namespace":"ring.examples","fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}`
	addReqSchema  = `{"type":"record","name":"AddReq","namespace":"ring.examples","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}`
	addRespSchema = `{"type":"record","name":"AddResp","namespace":"ring.examples","fields":[{"name":"sum","type":"long"}]}`
)

func repoBin(rel string) string {
	p, _ := filepath.Abs(filepath.Join("..", "..", "target", "debug", rel))
	return p
}

func startBroker(t *testing.T) *exec.Cmd {
	t.Helper()
	os.RemoveAll("/dev/shm/impulse-ring.ctl.v1")
	cmd := exec.Command(repoBin("impulsed"))
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := os.Stat("/dev/shm/impulse-ring.ctl.v1"); err == nil {
			time.Sleep(50 * time.Millisecond)
			return cmd
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("broker did not come up")
	return nil
}

func addHandler(req []byte) ([]byte, error) {
	d := NewDecoder(req)
	a, b := d.Long(), d.Long()
	e := NewEncoder()
	e.PutLong(a + b)
	return e.Bytes(), nil
}

func TestSelfFlow(t *testing.T) {
	broker := startBroker(t)
	defer func() {
		broker.Process.Signal(os.Interrupt)
		broker.Wait()
	}()

	svc, err := Connect("go-svc")
	if err != nil {
		t.Fatalf("connect svc: %v", err)
	}
	pub, err := svc.PublishChannel("go-metrics", metricSchema, "chan-key")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	me := NewEncoder()
	me.PutString("cpu")
	me.PutDouble(0.75)
	if err := pub.Publish(me.Bytes()); err != nil {
		t.Fatalf("publish msg: %v", err)
	}
	if err := svc.ExposeFunction("go-add", addReqSchema, addRespSchema, "fn-key", addHandler); err != nil {
		t.Fatalf("expose: %v", err)
	}

	cli, err := Connect("go-cli")
	if err != nil {
		t.Fatalf("connect cli: %v", err)
	}

	chans, err := cli.ListChannels()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var cid int64 = -1
	requiresKey := false
	for _, c := range chans {
		if c.Name == "go-metrics" {
			cid = c.ChannelID
			requiresKey = c.RequiresKey
		}
	}
	if cid < 0 {
		t.Fatal("channel not listed")
	}
	if !requiresKey {
		t.Error("channel should require a key")
	}

	if _, err := cli.Subscribe(cid, "wrong"); err == nil {
		t.Error("subscribe with wrong key should fail")
	}

	sub, err := cli.Subscribe(cid, "chan-key")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	body, err := sub.Recv(2000)
	if err != nil || body == nil {
		t.Fatalf("recv: %v", err)
	}
	d := NewDecoder(body)
	if name := d.String(); name != "cpu" {
		t.Errorf("metric name = %q, want cpu", name)
	}
	if v := d.Double(); v < 0.74 || v > 0.76 {
		t.Errorf("metric value = %v, want 0.75", v)
	}

	ae := NewEncoder()
	ae.PutLong(7)
	ae.PutLong(35)
	resp, err := cli.Call("go-add", "fn-key", ae.Bytes(), 5000)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if sum := NewDecoder(resp).Long(); sum != 42 {
		t.Errorf("add(7,35) = %d, want 42", sum)
	}

	if _, err := cli.Call("go-add", "nope", ae.Bytes(), 2000); err == nil {
		t.Error("call with wrong key should fail")
	}

	sub.Close()
	pub.Close()
	cli.Close()
	svc.Close()
}

// TestCrossLanguage subscribes to a channel published by the Rust peer and
// calls a function it exposes, proving Go<->Rust Avro data-plane interop.
func TestCrossLanguage(t *testing.T) {
	broker := startBroker(t)
	defer func() {
		broker.Process.Signal(os.Interrupt)
		broker.Wait()
	}()

	peer := exec.Command(repoBin("examples/peer"))
	peer.Stderr = os.Stderr
	if err := peer.Start(); err != nil {
		t.Fatalf("start peer: %v", err)
	}
	defer func() {
		peer.Process.Kill()
		peer.Wait()
	}()
	time.Sleep(600 * time.Millisecond)

	c, err := Connect("go-xlang")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	var cid int64 = -1
	for i := 0; i < 50 && cid < 0; i++ {
		chans, _ := c.ListChannels()
		for _, ch := range chans {
			if ch.Name == "rmetrics" {
				cid = ch.ChannelID
			}
		}
		if cid < 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if cid < 0 {
		t.Fatal("rust channel 'rmetrics' not found")
	}

	sub, err := c.Subscribe(cid, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	body, err := sub.Recv(3000)
	if err != nil || body == nil {
		t.Fatalf("recv from rust: %v", err)
	}
	d := NewDecoder(body)
	if name := d.String(); name != "temp" {
		t.Errorf("rust metric name = %q, want temp", name)
	}
	sub.Close()

	me := NewEncoder()
	me.PutLong(6)
	me.PutLong(7)
	resp, err := c.Call("rmul", "", me.Bytes(), 5000)
	if err != nil {
		t.Fatalf("call rust rmul: %v", err)
	}
	if product := NewDecoder(resp).Long(); product != 42 {
		t.Errorf("rmul(6,7) = %d, want 42", product)
	}
}
