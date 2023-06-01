#include <grpcpp/server.h>

#include <boost/asio.hpp>

#include "ray/common/rpc/function2.hpp"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

template <typename CompletionToken, typename Arg>
fu2::unique_function<void(Arg)> MakeCallback(CompletionToken &&handler) {
  using HandlerType = std::decay_t<decltype(handler)>;
  return [handler = std::forward<HandlerType>(handler)](Arg arg) mutable {
    auto alloc = boost::asio::get_associated_allocator(
        handler, boost::asio::recycling_allocator<void>());
    auto ex = boost::asio::get_associated_executor(handler);
    RAY_CHECK(ex) << "Invalid executor";
    boost::asio::dispatch(
        ex, boost::asio::bind_allocator(alloc, std::bind(std::move(handler), arg)));
  };
}

template <typename TRequest, typename TResponse, typename TReactor>
struct RayReactor : public TReactor {
  using Request = TRequest;
  using Response = TResponse;
  using Reactor = TReactor;

  void Deref() {
    if (--ref_count_ == 0) {
      RAY_LOG(INFO) << "Destroying reactor";
      delete this;
    }
  }

 private:
  // One ref for the shared ptr and the other ond for gRPC.
  std::atomic<int64_t> ref_count_{2};
};

template <typename TReactor>
class RayServerUnaryReactor : virtual public TReactor {
 public:
  RayServerUnaryReactor(grpc::CallbackServerContext *server_context)
      : server_context_(server_context) {}
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;
  virtual ~RayServerUnaryReactor() {
    RAY_LOG(ERROR) << "RayServerUnaryReactor destroyed";
  }

  void Finish(const grpc::Status &status = grpc::Status::OK) {
    RAY_LOG(ERROR) << "Server::Finish";
    if (!finished_) {
      finished_ = true;
      Reactor::Finish(status);
    }
  }

  grpc::CallbackServerContext &GetContext() { return *server_context_; }

  template <typename CompletionToken>
  auto SendInitialMetadata(CompletionToken &&token) {
    RAY_CHECK(!send_metadata_cb_) << "There SendInitialMetadata has already called";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      send_metadata_cb_ =
          MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      this->StartSendInitialMetadata();
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token));
  }

 private:
  void OnDone() override {
    RAY_LOG(ERROR) << "ServerOnDone";
    this->Deref();
  }

  void OnCancel() override {
    RAY_LOG(ERROR) << "Server:OnCancel";
    Finish(grpc::Status::CANCELLED);
  }

  void OnSendInitialMetadataDone(bool ok) override {
    if (send_metadata_cb_) {
      auto f = std::move(send_metadata_cb_);
      f(ok);
    }
  }

  template <typename Reactor>
  friend std::shared_ptr<Reactor> MakeServerReactor(grpc::CallbackServerContext *);

  bool finished_ = false;
  grpc::CallbackServerContext *server_context_;
  fu2::unique_function<void(bool)> send_metadata_cb_;
};

template <typename TReactor>
class RayServerWriteReactor : virtual public RayServerUnaryReactor<TReactor> {
 public:
  using Base = RayServerUnaryReactor<TReactor>;
  using Response = typename Base::Response;

  RayServerWriteReactor(grpc::CallbackServerContext *server_context)
      : RayServerUnaryReactor<TReactor>(server_context) {}

  template <typename CompletionToken>
  auto Write(const Response &response, CompletionToken &&token) {
    return Write(response, grpc::WriteOptions(), std::move(token));
  }

  template <typename CompletionToken>
  auto Write(const Response &response, grpc::WriteOptions opts, CompletionToken &&token) {
    RAY_CHECK(!write_callback_) << "There is a pending write";
    auto init = [this, opts = std::move(opts)](auto &&handler,
                                               const auto &response) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      write_callback_ =
          MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      this->StartWrite(&response, opts);
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), response);
  }

 private:
  void OnWriteDone(bool ok) override {
    RAY_CHECK(write_callback_);
    auto f = std::move(write_callback_);
    f(ok);
  }

  fu2::unique_function<void(bool)> write_callback_;
};

template <typename TReactor>
class RayServerReadReactor : virtual public RayServerUnaryReactor<TReactor> {
 public:
  using Base = RayServerUnaryReactor<TReactor>;
  using Request = typename Base::Request;

  RayServerReadReactor(grpc::CallbackServerContext *server_context)
      : RayServerUnaryReactor<TReactor>(server_context) {}

  template <typename CompletionToken>
  auto Read(CompletionToken &&token) {
    RAY_CHECK(!read_callback_);
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      read_callback_ = MakeCallback<HandlerType, std::pair<Request, bool>>(
          std::forward<HandlerType>(handler));
      this->StartRead(&request_);
    };

    return boost::asio::async_initiate<CompletionToken, void(std::pair<Request, bool>)>(
        init, std::forward<CompletionToken>(token));
  }

 private:
  void OnReadDone(bool ok) override {
    RAY_CHECK(read_callback_);
    auto f = std::move(read_callback_);
    f(std::make_pair(std::move(request_), ok));
  }

  Request request_;
  fu2::unique_function<void(std::pair<Request, bool>)> read_callback_;
};

template <typename TReactor>
class RayServerBidiReactor : public RayServerReadReactor<TReactor>,
                             public RayServerWriteReactor<TReactor> {
 public:
  using ReadBase = RayServerReadReactor<TReactor>;
  using WriteBase = RayServerWriteReactor<TReactor>;
  static_assert(std::is_same_v<typename ReadBase::Base, typename WriteBase::Base>);
  using Base = typename ReadBase::Base;
  using Request = typename ReadBase::Response;
  using Response = typename WriteBase::Request;
  RayServerBidiReactor(grpc::CallbackServerContext *server_context)
      : Base(server_context), ReadBase(server_context), WriteBase(server_context) {}
};

template <typename TReactor>
class RayClientUnaryReactor : virtual public TReactor {
 public:
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;

  virtual ~RayClientUnaryReactor() {
    RAY_LOG(ERROR) << "RayClientUnaryReactor destroyed";
  }

  grpc::ClientContext &GetContext() { return client_context_; }

  template <typename CompletionToken>
  auto ReadServerInitialMetadata(CompletionToken &&token) {
    RAY_CHECK(!read_metadata_cb_) << "The GetServerInitialMetadata has already called";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      read_metadata_cb_ =
          MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      this->StartReadInitialMetadata();
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token));
  }

  template <typename CompletionToken>
  auto AsyncWait(CompletionToken &&token) {
    RAY_CHECK(!wait_callback_) << "The AsyncWait has already called";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      auto f =
          MakeCallback<HandlerType, grpc::Status>(std::forward<HandlerType>(handler));
      absl::MutexLock lock(&wait_callback_mutex_);
      if (status_.has_value()) {
        f(*status_);
      } else {
        wait_callback_ = std::move(f);
      }
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token));
  }
  void StartCall() {
    // Increase the ref count to make sure the reactor is alive until the call is done.
    // This will create a cirtular, but it'll be broken when OnDone is called in gRPC.
    RAY_LOG(ERROR) << "hold_count_= " << hold_count_;
    Reactor::AddMultipleHolds(hold_count_);
    Reactor::StartCall();
  }

 protected:
  void AddHold() { ++hold_count_; }

  void RemoveHold() {
    --hold_count_;
    Reactor::RemoveHold();
  }

  void ClearHold() {
    while (hold_count_ > 0) {
      RemoveHold();
    }
  }

  void OnDone(const grpc::Status &status) override {
    {
      absl::MutexLock lock(&wait_callback_mutex_);
      if (wait_callback_) {
        auto f = std::move(wait_callback_);
        f(status);
      } else {
        status_ = status;
      }
    }
    this->Deref();
  }

  void OnReadInitialMetadataDone(bool ok) override {
    if (read_metadata_cb_) {
      auto f = std::move(read_metadata_cb_);
      f(ok);
    }
  }

  template <typename Reactor>
  friend std::shared_ptr<Reactor> MakeClientReactor();

  bool done_ = false;
  int hold_count_ = 0;
  grpc::ClientContext client_context_;
  fu2::unique_function<void(bool)> read_metadata_cb_;

  absl::Mutex wait_callback_mutex_;

  std::optional<grpc::Status> status_ GUARDED_BY(wait_callback_mutex_);
  fu2::unique_function<void(grpc::Status)> wait_callback_
      GUARDED_BY(wait_callback_mutex_);
};

template <typename TReactor>
class RayClientWriteReactor : virtual public RayClientUnaryReactor<TReactor> {
 public:
  using Base = RayClientUnaryReactor<TReactor>;
  using Request = typename Base::Request;

  RayClientWriteReactor() { Base::AddHold(); }

  template <typename CompletionToken>
  auto Write(const Request &request, CompletionToken &&token) {
    return Write(request, grpc::WriteOptions(), std::move(token));
  }

  template <typename CompletionToken>
  auto Write(const Request &request, grpc::WriteOptions opts, CompletionToken &&token) {
    RAY_CHECK(!write_callback_) << "There is a pending write";
    auto init = [this, opts = std::move(opts)](auto &&handler,
                                               const auto &request) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      write_callback_ =
          MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      RAY_LOG(ERROR) << "ClientStartWrite";
      this->StartWrite(&request, opts);
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), request);
  }

  template <typename CompletionToken>
  auto WritesDone(CompletionToken &&token) {
    RAY_CHECK(!write_callback_) << "There is a pending write";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      write_done_callback_ =
          MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      this->StartWritesDone();
    };
  }

 protected:
  void OnWritesDoneDone(bool ok) override {
    RAY_LOG(ERROR) << "WriteDoneDone";
    auto f = std::move(write_done_callback_);
    f(ok);
    if (!ok) {
      Base::RemoveHold();
    }
  }

  void OnWriteDone(bool ok) override {
    RAY_LOG(ERROR) << "ClientWriteDone: " << ok;
    RAY_CHECK(write_callback_);
    auto f = std::move(write_callback_);
    f(ok);
    if (!ok) {
      Base::RemoveHold();
    }
  }

  fu2::unique_function<void(bool)> write_done_callback_;
  fu2::unique_function<void(bool)> write_callback_;
};

template <typename TReactor>
class RayClientReadReactor : virtual public RayClientUnaryReactor<TReactor> {
 public:
  using Base = RayClientUnaryReactor<TReactor>;
  using Response = typename Base::Response;
  RayClientReadReactor() { Base::AddHold(); }

  template <typename CompletionToken>
  auto Read(CompletionToken &&token) {
    RAY_CHECK(!read_callback_);
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      read_callback_ = MakeCallback<HandlerType, std::pair<Response, bool>>(
          std::forward<HandlerType>(handler));
      this->StartRead(&response_);
    };
    return boost::asio::async_initiate<CompletionToken, void(std::pair<Response, bool>)>(
        init, std::forward<CompletionToken>(token));
  }

 protected:
  void OnReadDone(bool ok) override {
    RAY_LOG(ERROR) << "ClientReadDone: " << ok;
    RAY_CHECK(read_callback_);
    auto f = std::move(read_callback_);
    f(std::make_pair(std::move(response_), ok));
    if (!ok) {
      Base::RemoveHold();
    }
  }

  Response response_;
  fu2::unique_function<void(std::pair<Response, bool>)> read_callback_;
};

template <typename TReactor>
class RayClientBidiReactor : virtual public RayClientReadReactor<TReactor>,
                             virtual public RayClientWriteReactor<TReactor> {
  using ReadBase = RayClientReadReactor<TReactor>;
  using WriteBase = RayClientWriteReactor<TReactor>;
  static_assert(std::is_same_v<typename ReadBase::Base, typename WriteBase::Base>);
  using Base = typename ReadBase::Base;

  using Response = typename ReadBase::Response;
  using Request = typename WriteBase::Request;
};

template <typename Reactor>
std::shared_ptr<Reactor> MakeServerReactor(grpc::CallbackServerContext *context) {
  return std::shared_ptr<Reactor>(new Reactor(context), [](auto p) {
    p->Finish();
    p->Deref();
  });
}

template <typename Reactor>
std::shared_ptr<Reactor> MakeClientReactor() {
  return std::shared_ptr<Reactor>(new Reactor(), [](auto p) {
    RAY_LOG(ERROR) << "Client::Deref";
    p->ClearHold();
    p->Deref();
  });
}

}  // namespace rpc
}  // namespace ray
