// Ring relay benchmark — Go node.
// Usage: bench_go <index> <num_services> <laps>
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	ring "github.com/goidago/impulse-ring/connectors/go"
)

const key = "ring-bench-key"
const schema = `{"type":"record","name":"BenchToken","namespace":"ring.bench","fields":[` +
	`{"name":"lap","type":"long"},{"name":"start_nanos","type":"long"},` +
	`{"name":"elapsed_ns","type":"long"},{"name":"stop","type":"boolean"}]}`

func encode(lap, start, elapsed int64, stop bool) []byte {
	e := ring.NewEncoder()
	e.PutLong(lap)
	e.PutLong(start)
	e.PutLong(elapsed)
	e.PutBool(stop)
	return e.Bytes()
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: bench_go <index> <num_services> <laps>")
		os.Exit(2)
	}
	index, _ := strconv.Atoi(os.Args[1])
	n, _ := strconv.Atoi(os.Args[2])
	laps, _ := strconv.ParseInt(os.Args[3], 10, 64)
	self := fmt.Sprintf("bench-%d", index)

	conn, err := ring.Connect(self)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	pub, err := conn.PublishChannel(self, schema, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "publish:", err)
		os.Exit(1)
	}

	prev := fmt.Sprintf("bench-%d", (index+n-1)%n)
	var cid int64 = -1
	for cid < 0 {
		for _, c := range mustList(conn) {
			if c.Name == prev {
				cid = c.ChannelID
			}
		}
		if cid < 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	sub, err := conn.Subscribe(cid, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "subscribe:", err)
		os.Exit(1)
	}

	if index == 0 {
		for present(conn, n) < n {
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)

		t0 := time.Now()
		pub.Publish(encode(0, t0.UnixNano(), 0, false))
		for {
			body, _ := sub.Recv(30000)
			if body == nil {
				break
			}
			lap := ring.NewDecoder(body).Long() + 1
			if lap >= laps {
				elapsed := time.Since(t0)
				pub.Publish(encode(lap, t0.UnixNano(), elapsed.Nanoseconds(), true))
				secs := elapsed.Seconds()
				fmt.Printf("ring-bench(go): %d laps across %d services in %.3fs | %.0f laps/s | %d ns/lap | %d ns/hop\n",
					laps, n, secs, float64(laps)/secs, elapsed.Nanoseconds()/laps, elapsed.Nanoseconds()/(laps*int64(n)))
				time.Sleep(300 * time.Millisecond)
				break
			}
			pub.Publish(encode(lap, t0.UnixNano(), 0, false))
		}
	} else {
		for {
			body, _ := sub.Recv(30000)
			if body == nil {
				break
			}
			d := ring.NewDecoder(body)
			d.Long()
			d.Long()
			d.Long()
			stop := d.Bool()
			pub.Publish(body) // forward the same bytes
			if stop {
				break
			}
		}
	}

	sub.Close()
	pub.Close()
	conn.Close()
}

func mustList(c *ring.Connection) []ring.ChannelInfo {
	l, _ := c.ListChannels()
	return l
}

func present(c *ring.Connection, n int) int {
	chans := mustList(c)
	count := 0
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("bench-%d", i)
		for _, ch := range chans {
			if ch.Name == name {
				count++
				break
			}
		}
	}
	return count
}
