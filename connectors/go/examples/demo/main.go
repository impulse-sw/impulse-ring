// Demo of the Go connector. Start the broker first:
//
//	cargo run -p impulsed
//	go run ./connectors/go/examples/demo
package main

import (
	"fmt"
	"os"

	ring "github.com/goidago/impulse-ring/connectors/go"
)

const (
	metricSchema = `{"type":"record","name":"Metric","namespace":"ring.examples",
		"fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}`
	addReqSchema = `{"type":"record","name":"AddReq","namespace":"ring.examples",
		"fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}`
	addRespSchema = `{"type":"record","name":"AddResp","namespace":"ring.examples",
		"fields":[{"name":"sum","type":"long"}]}`
)

func add(req []byte) ([]byte, error) {
	d := ring.NewDecoder(req)
	a, b := d.Long(), d.Long()
	e := ring.NewEncoder()
	e.PutLong(a + b)
	return e.Bytes(), nil
}

func main() {
	svc, err := ring.Connect("go-demo-svc")
	if err != nil {
		fmt.Fprintln(os.Stderr, "broker not running?", err)
		os.Exit(1)
	}
	defer svc.Close()

	pub, err := svc.PublishChannel("go-metrics", metricSchema, "")
	if err != nil {
		panic(err)
	}
	me := ring.NewEncoder()
	me.PutString("cpu")
	me.PutDouble(0.5)
	pub.Publish(me.Bytes())

	if err := svc.ExposeFunction("go-add", addReqSchema, addRespSchema, "", add); err != nil {
		panic(err)
	}

	cli, err := ring.Connect("go-demo-cli")
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	chans, _ := cli.ListChannels()
	for _, c := range chans {
		fmt.Printf("channel: %s (requires_key=%v)\n", c.Name, c.RequiresKey)
	}

	ae := ring.NewEncoder()
	ae.PutLong(20)
	ae.PutLong(22)
	resp, err := cli.Call("go-add", "", ae.Bytes(), 5000)
	if err != nil {
		fmt.Fprintln(os.Stderr, "call failed:", err)
		os.Exit(1)
	}
	fmt.Println("add(20, 22) =", ring.NewDecoder(resp).Long())
}
