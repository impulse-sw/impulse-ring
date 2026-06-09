// Ring — idiomatic C++ wrapper (header-only) over the native C connector.
//
// This is intentionally a thin RAII/STL wrapper around the C ABI in
// <impulse_ring.h>; it does NOT reimplement the protocol. C++ users get
// std::string / std::vector / std::function and exceptions while reusing the
// proven C core. Link against the `impulse_ring` C library.
//
// Linux only (Tier 0: arm64/amd64).
#ifndef IMPULSE_RING_HPP
#define IMPULSE_RING_HPP

#include "impulse_ring.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace impulse_ring {

using Bytes = std::vector<uint8_t>;

// Thrown on any connector error (connect/publish/subscribe/expose/call).
class Error : public std::runtime_error {
public:
  explicit Error(const std::string &what) : std::runtime_error(what) {}
};

// ---- Avro datum writer / reader (wrap the C primitives) ----

class AvroWriter {
public:
  AvroWriter() : w_(ir_avro_w_new()) {
    if (!w_)
      throw Error("ir_avro_w_new failed");
  }
  ~AvroWriter() { ir_avro_w_free(w_); }
  AvroWriter(const AvroWriter &) = delete;
  AvroWriter &operator=(const AvroWriter &) = delete;

  AvroWriter &put_bool(bool v) { return (ir_avro_put_boolean(w_, v), *this); }
  AvroWriter &put_int(int32_t v) { return (ir_avro_put_int(w_, v), *this); }
  AvroWriter &put_long(int64_t v) { return (ir_avro_put_long(w_, v), *this); }
  AvroWriter &put_float(float v) { return (ir_avro_put_float(w_, v), *this); }
  AvroWriter &put_double(double v) { return (ir_avro_put_double(w_, v), *this); }
  AvroWriter &put_string(const std::string &s) { return (ir_avro_put_string(w_, s.c_str()), *this); }
  AvroWriter &put_bytes(const Bytes &b) { return (ir_avro_put_bytes(w_, b.data(), b.size()), *this); }
  AvroWriter &array_start(int64_t count) { return (ir_avro_array_start(w_, count), *this); }
  AvroWriter &array_end() { return (ir_avro_array_end(w_), *this); }

  Bytes bytes() const {
    size_t n = 0;
    const uint8_t *p = ir_avro_w_bytes(w_, &n);
    return Bytes(p, p + n);
  }

private:
  ir_avro_w *w_;
};

class AvroReader {
public:
  AvroReader(const uint8_t *data, size_t len) : r_(ir_avro_r_new(data, len)) {
    if (!r_)
      throw Error("ir_avro_r_new failed");
  }
  explicit AvroReader(const Bytes &b) : AvroReader(b.data(), b.size()) {}
  ~AvroReader() { ir_avro_r_free(r_); }
  AvroReader(const AvroReader &) = delete;
  AvroReader &operator=(const AvroReader &) = delete;

  bool get_bool() { return ir_avro_get_boolean(r_) != 0; }
  int32_t get_int() { return ir_avro_get_int(r_); }
  int64_t get_long() { return ir_avro_get_long(r_); }
  float get_float() { return ir_avro_get_float(r_); }
  double get_double() { return ir_avro_get_double(r_); }
  int64_t array_count() { return ir_avro_get_array_count(r_); }
  std::string get_string() {
    char *s = ir_avro_get_string(r_);
    std::string out(s ? s : "");
    ir_free(s);
    return out;
  }

private:
  ir_avro_r *r_;
};

struct ChannelInfo {
  int64_t channel_id;
  std::string name;
  std::string owner_app;
  uint64_t schema_fp;
  bool requires_key;
};

class Publisher {
public:
  explicit Publisher(ir_publisher *p) : p_(p) {}
  ~Publisher() { ir_publisher_free(p_); }
  Publisher(Publisher &&o) noexcept : p_(o.p_) { o.p_ = nullptr; }
  Publisher(const Publisher &) = delete;
  Publisher &operator=(const Publisher &) = delete;

  void publish(const Bytes &body) {
    if (ir_publish(p_, body.data(), body.size()) != IR_OK)
      throw Error("publish failed");
  }

private:
  ir_publisher *p_;
};

class Subscriber {
public:
  explicit Subscriber(ir_subscriber *s) : s_(s) {}
  ~Subscriber() { ir_subscriber_free(s_); }
  Subscriber(Subscriber &&o) noexcept : s_(o.s_) { o.s_ = nullptr; }
  Subscriber(const Subscriber &) = delete;
  Subscriber &operator=(const Subscriber &) = delete;

  // Returns the next message body, or std::nullopt on timeout.
  std::optional<Bytes> recv(int timeout_ms) {
    uint8_t *body = nullptr;
    size_t len = 0;
    int rc = ir_recv(s_, timeout_ms, &body, &len);
    if (rc == 0)
      return std::nullopt;
    if (rc < 0)
      throw Error("recv failed");
    Bytes out(body, body + len);
    ir_free(body);
    return out;
  }

  uint64_t schema_fp() const { return ir_subscriber_schema_fp(s_); }

private:
  ir_subscriber *s_;
};

// handler(req) -> response body.
using Handler = std::function<Bytes(const uint8_t *req, size_t len)>;

class Connection {
public:
  explicit Connection(const std::string &app_name) : c_(ir_connect(app_name.c_str())) {
    if (!c_)
      throw Error("connect failed (is impulsed running?)");
  }
  ~Connection() {
    if (c_)
      ir_disconnect(c_);
  }
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

  Publisher publish_channel(const std::string &name, const std::string &schema_json, const std::string &key = "") {
    ir_publisher *p = ir_publish_channel(c_, name.c_str(), schema_json.c_str(), key.empty() ? nullptr : key.c_str());
    if (!p)
      throw Error(std::string("publish_channel: ") + ir_last_error(c_));
    return Publisher(p);
  }

  std::vector<ChannelInfo> list_channels() {
    std::vector<ir_channel_info> raw(64);
    size_t count = 0;
    if (ir_list_channels(c_, raw.data(), raw.size(), &count) != IR_OK)
      throw Error(std::string("list_channels: ") + ir_last_error(c_));
    std::vector<ChannelInfo> out;
    out.reserve(count);
    for (size_t i = 0; i < count; i++)
      out.push_back({raw[i].channel_id, raw[i].name, raw[i].owner_app, raw[i].schema_fp, raw[i].requires_key != 0});
    return out;
  }

  Subscriber subscribe(int64_t channel_id, const std::string &key = "") {
    ir_subscriber *s = ir_subscribe(c_, channel_id, key.empty() ? nullptr : key.c_str());
    if (!s)
      throw Error(std::string("subscribe: ") + ir_last_error(c_));
    return Subscriber(s);
  }

  void expose_function(const std::string &name, const std::string &req_schema, const std::string &resp_schema,
                       Handler handler, const std::string &key = "") {
    auto box = std::make_unique<Handler>(std::move(handler));
    int rc = ir_expose_function(c_, name.c_str(), req_schema.c_str(), resp_schema.c_str(),
                                key.empty() ? nullptr : key.c_str(), &Connection::trampoline, box.get());
    if (rc != IR_OK)
      throw Error(std::string("expose_function: ") + ir_last_error(c_));
    handlers_.push_back(std::move(box)); // keep the std::function alive for the service thread
  }

  Bytes call(const std::string &fn_name, const Bytes &req, const std::string &key = "", int timeout_ms = 5000) {
    uint8_t *resp = nullptr;
    size_t resp_len = 0;
    int rc = ir_call(c_, fn_name.c_str(), key.empty() ? nullptr : key.c_str(), req.data(), req.size(), timeout_ms,
                     &resp, &resp_len);
    if (rc != IR_OK)
      throw Error(std::string("call: ") + ir_last_error(c_));
    Bytes out(resp, resp + resp_len);
    ir_free(resp);
    return out;
  }

private:
  static int trampoline(const uint8_t *req, size_t req_len, uint8_t **resp, size_t *resp_len, void *user) {
    auto *fn = static_cast<Handler *>(user);
    try {
      Bytes out = (*fn)(req, req_len);
      uint8_t *p = static_cast<uint8_t *>(malloc(out.empty() ? 1 : out.size()));
      if (!p)
        return 1;
      std::memcpy(p, out.data(), out.size());
      *resp = p;
      *resp_len = out.size();
      return 0;
    } catch (...) {
      return 1;
    }
  }

  ir_conn *c_;
  std::vector<std::unique_ptr<Handler>> handlers_;
};

} // namespace impulse_ring

#endif // IMPULSE_RING_HPP
