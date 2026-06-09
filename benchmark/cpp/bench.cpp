// Ring relay benchmark — C++ node (uses the header-only C++ wrapper).
// Usage: bench_cpp <index> <num_services> <laps>
#include "impulse_ring.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace ir = impulse_ring;

static const char *KEY = "ring-bench-key";
static const char *SCHEMA =
    R"({"type":"record","name":"BenchToken","namespace":"ring.bench","fields":[)"
    R"({"name":"lap","type":"long"},{"name":"start_nanos","type":"long"},)"
    R"({"name":"elapsed_ns","type":"long"},{"name":"stop","type":"boolean"}]})";

static int64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

static ir::Bytes encode(int64_t lap, int64_t start, int64_t elapsed, bool stop) {
  ir::AvroWriter w;
  w.put_long(lap).put_long(start).put_long(elapsed).put_bool(stop);
  return w.bytes();
}

int main(int argc, char **argv) {
  if (argc < 4) {
    std::fprintf(stderr, "usage: bench_cpp <index> <num_services> <laps>\n");
    return 2;
  }
  int index = std::atoi(argv[1]);
  int n = std::atoi(argv[2]);
  long long laps = std::atoll(argv[3]);
  std::string self = "bench-" + std::to_string(index);

  try {
    ir::Connection conn(self);
    auto pub = conn.publish_channel(self, SCHEMA, KEY);

    std::string prev = "bench-" + std::to_string((index + n - 1) % n);
    int64_t cid = -1;
    while (cid < 0) {
      for (const auto &c : conn.list_channels())
        if (c.name == prev)
          cid = c.channel_id;
      if (cid < 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    auto sub = conn.subscribe(cid, KEY);

    if (index == 0) {
      for (;;) {
        int up = 0;
        auto chans = conn.list_channels();
        for (int i = 0; i < n; i++) {
          std::string name = "bench-" + std::to_string(i);
          for (const auto &c : chans)
            if (c.name == name) {
              up++;
              break;
            }
        }
        if (up >= n)
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));

      int64_t t0 = now_ns();
      pub.publish(encode(0, t0, 0, false));
      for (;;) {
        auto body = sub.recv(30000);
        if (!body)
          break;
        int64_t lap = ir::AvroReader(*body).get_long() + 1;
        if (lap >= laps) {
          int64_t elapsed = now_ns() - t0;
          pub.publish(encode(lap, t0, elapsed, true));
          double secs = (double)elapsed / 1e9;
          std::printf("ring-bench(cpp): %lld laps across %d services in %.3fs | %.0f laps/s | %lld ns/lap | %lld "
                      "ns/hop\n",
                      laps, n, secs, (double)laps / secs, (long long)(elapsed / laps), (long long)(elapsed / (laps * n)));
          std::this_thread::sleep_for(std::chrono::milliseconds(300));
          break;
        }
        pub.publish(encode(lap, t0, 0, false));
      }
    } else {
      for (;;) {
        auto body = sub.recv(30000);
        if (!body)
          break;
        ir::AvroReader r(*body);
        r.get_long();
        r.get_long();
        r.get_long();
        bool stop = r.get_bool();
        pub.publish(*body); // forward the same bytes
        if (stop)
          break;
      }
    }
  } catch (const ir::Error &e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
  return 0;
}
