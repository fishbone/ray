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
};

template <typename TReactor>
class RayServerUnaryReactor : virtual public TReactor {
 public:
  RayServerUnaryReactor(grpc::CallbackServerContext *server_context)
      : server_context_(server_context) {}
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;

  void Finish(const grpc::Status &status) { Reactor::Finish(status); }

  grpc::CallbackServerContext &GetContext() { return *server_context_; }

  template <typename CompletionToken>
  auto SendInitialMetadata(CompletionToken &&token) {
    RAY_CHECK(!send_metadata_cb_) << "There SendInitialMetadata has already called";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      send_metadata_cb_ =
          MakeCallback<HandlerType, grpc::Status>(std::forward<HandlerType>(handler));
      this->StartSendInitialMetadata();
    };

    return boost::asio::async_initiate<CompletionToken, void(grpc::Status)>(
        init, std::forward<CompletionToken>(token));
  }

 private:
  void OnDone() override { delete this; }

  void OnCancel() override { Finish(grpc::Status::CANCELLED); }

  void OnSendInitialMetadataDone(bool ok) override {
    if (send_metadata_cb_) {
      auto f = std::move(send_metadata_cb_);
      if (ok) {
        f(grpc::Status::OK);
      } else {
        if (server_context_->IsCancelled()) {
          f(grpc::Status::CANCELLED);
        } else {
          RAY_LOG(WARNING) << "Unexpected error sending initial metadata";
          f(grpc::Status(grpc::StatusCode::INTERNAL, "Failed to send initial metadata"));
        }
      }
    }
  }

  grpc::CallbackServerContext *server_context_;
  fu2::unique_function<void(grpc::Status)> send_metadata_cb_;
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
          MakeCallback<HandlerType, grpc::Status>(std::forward<HandlerType>(handler));
      this->StartWrite(&response, opts);
    };

    return boost::asio::async_initiate<CompletionToken, void(grpc::Status)>(
        init, std::forward<CompletionToken>(token), response);
  }

 private:
  void OnWriteDone(bool ok) override {
    RAY_CHECK(write_callback_);
    auto f = std::move(write_callback_);
    if (ok) {
      f(grpc::Status::OK);
    } else {
      if (this->GetContext().IsCancelled()) {
        f(grpc::Status::CANCELLED);
      } else {
        RAY_LOG(WARNING) << "Unexpected error sending response";
        f(grpc::Status(grpc::StatusCode::INTERNAL, "Failed to send response"));
      }
    }
  }

  fu2::unique_function<void(grpc::Status)> write_callback_;
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
      read_callback_ = MakeCallback<HandlerType, std::pair<Request, grpc::Status>>(
          std::forward<HandlerType>(handler));
      this->StartRead(&request_);
    };

    return boost::asio::async_initiate<CompletionToken,
                                       void(std::pair<Request, grpc::Status>)>(
        init, std::forward<CompletionToken>(token));
  }

 private:
  void OnReadDone(bool ok) override {
    RAY_CHECK(read_callback_);
    auto f = std::move(read_callback_);
    grpc::Status status;
    if (!ok) {
      if (this->GetContext().IsCancelled()) {
        status = grpc::Status::CANCELLED;
      } else {
        status = grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "EOF");
      }
    }
    f(std::make_pair(std::move(request_), std::move(status)));
  }

  Request request_;
  fu2::unique_function<void(std::pair<Request, grpc::Status>)> read_callback_;
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
class RayClientUnaryReactor
    : virtual public TReactor,
      public std::enable_shared_from_this<RayClientUnaryReactor<TReactor>> {
 public:
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;

  grpc::ClientContext &GetContext() { return client_context_; }

  template <typename CompletionToken>
  auto ReadServerInitialMetadata(CompletionToken &&token) {
    RAY_CHECK(!read_metadata_cb_) << "The GetServerInitialMetadata has already called";
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      read_metadata_cb_ =
          MakeCallback<HandlerType, grpc::Status>(std::forward<HandlerType>(handler));
      this->StartReadInitialMetadata();
    };

    return boost::asio::async_initiate<CompletionToken, void(grpc::Status)>(
        init, std::forward<CompletionToken>(token));
  }

  void StartCall() {
    // Increase the ref count to make sure the reactor is alive until the call is done.
    // This will create a cirtular, but it'll be broken when OnDone is called in gRPC.
    self_ = this->shared_from_this();
    Reactor::StartCall();
  }

 private:
  void OnDone(const grpc::Status &status) override {
    if (read_metadata_cb_) {
      auto f = std::move(read_metadata_cb_);
      f(status);
    }
    self_ = nullptr;
  }

  void OnReadInitialMetadataDone(bool ok) override {
    if (ok && read_metadata_cb_) {
      auto f = std::move(read_metadata_cb_);
      f(grpc::Status::OK);
    }
  }

  std::shared_ptr<RayClientUnaryReactor> self_;
  grpc::ClientContext client_context_;
  fu2::unique_function<void(grpc::Status)> read_metadata_cb_;
};

template <typename TReactor>
class RayClientWriteReactor : virtual public RayClientUnaryReactor<TReactor> {
 public:
  using Base = RayClientUnaryReactor<TReactor>;
  using Request = typename Base::Request;

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
          MakeCallback<HandlerType, grpc::Status>(std::forward<HandlerType>(handler));
      this->StartWrite(&request, opts);
    };

    return boost::asio::async_initiate<CompletionToken, void(grpc::Status)>(
        init, std::forward<CompletionToken>(token), request);
  }

 protected:
  void OnDone(const grpc::Status &status) override {
    if (write_callback_) {
      auto f = std::move(write_callback_);
      write_callback_(status);
    }
  }

  void OnWriteDone(bool ok) override {
    if (ok) {
      RAY_CHECK(write_callback_);
      auto f = std::move(write_callback_);
      f(grpc::Status::OK);
    }
  }

  fu2::unique_function<void(grpc::Status)> write_callback_;
};

template <typename TReactor>
class RayClientReadReactor : virtual public RayClientUnaryReactor<TReactor> {
 public:
  using Base = RayClientUnaryReactor<TReactor>;
  using Response = typename Base::Response;

  template <typename CompletionToken>
  auto Read(CompletionToken &&token) {
    RAY_CHECK(!read_callback_);
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      read_callback_ = MakeCallback<HandlerType, std::pair<Response, grpc::Status>>(
          std::forward<HandlerType>(handler));
      this->StartRead(&response_);
    };
    return boost::asio::async_initiate<CompletionToken,
                                       void(std::pair<Response, grpc::Status>)>(
        init, std::forward<CompletionToken>(token));
  }

 protected:
  void OnDone(const grpc::Status &status) override {
    if (read_callback_) {
      auto f = std::move(read_callback_);
      f(std::make_pair(Response(), status));
    }
  }

  void OnReadDone(bool ok) override {
    RAY_CHECK(read_callback_);
    if (ok) {
      auto f = std::move(read_callback_);
      f(std::make_pair(Response(), grpc::Status::OK));
    }
  }

  Response response_;
  fu2::unique_function<void(std::pair<Response, grpc::Status>)> read_callback_;
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

 protected:
  void OnDone(const grpc::Status &status) override {
    ReadBase::OnDone(status);
    WriteBase::OnDone(status);
  }
};

}  // namespace rpc
}  // namespace ray
