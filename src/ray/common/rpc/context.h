#include <grpcpp/server.h>

#include <boost/asio.hpp>

#include "ray/common/rpc/function2.hpp"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {
/*
template<typename CompletionToken, typename ... Args>
auto MakeCallback(CompletionToken&& token, Args &&args...) {
  return [&&token = std::forward<CompletionToken>(token),
          args = std::make_tuple(std::forward<Args>(args) ...)]() mutable {
    auto alloc = boost::asio::get_associated_allocator(token,
boost::asio::recycling_allocator<void>()); auto ex =
boost::asio::get_associated_executor(token); RAY_CHECK(ex) << "Invalid executor"; auto
completion = [ boost::asio::dispatch( ex,

  }
}
*/
template <typename Request,
          typename Response,
          template <typename, typename>
          typename Reactor>
class RpcReactorBase : public Reactor<Request, Response> {
  using Base = Reactor<Request, Response>;

 public:
  template <typename CompletionToken, typename R>
  auto Read(R &request, CompletionToken &&token) {
    RAY_CHECK(!read_callback_);
    auto init = [this](auto &&handler, auto *request) mutable {
      read_callback_ = [handler =
                            std::forward<decltype(handler)>(handler)](bool ok) mutable {
        auto alloc = boost::asio::get_associated_allocator(
            handler, boost::asio::recycling_allocator<void>());
        auto ex = boost::asio::get_associated_executor(handler);
        RAY_CHECK(ex) << "Invalid executor";
        boost::asio::dispatch(
            ex,
            boost::asio::bind_allocator(
                alloc, [handler = std::forward<decltype(handler)>(handler), ok]() mutable {
                  handler(ok);
                }));
      };
      Base::StartRead(request);
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), &request);
  }

  template <typename CompletionToken, typename R>
  auto Write(const R &response, CompletionToken &&token) {
    return Write(response, grpc::WriteOptions(), std::move(token));
  }

  template <typename CompletionToken, typename R>
  auto Write(const R &response, grpc::WriteOptions opts, CompletionToken &&token) {
    RAY_CHECK(!write_callback_);
    auto init = [this, opts = std::move(opts)](auto &&handler,
                                               const auto &response) mutable {
      write_callback_ = [handler =
                             std::forward<decltype(handler)>(handler)](bool ok) mutable {
        auto alloc = boost::asio::get_associated_allocator(
            handler, boost::asio::recycling_allocator<void>());
        auto ex = boost::asio::get_associated_executor(handler);
        RAY_CHECK(ex) << "Invalid executor";
        boost::asio::dispatch(
            ex,
            boost::asio::bind_allocator(
                alloc,
                [handler = std::forward<decltype(handler)>(handler), ok]() mutable {
                  std::forward<decltype(handler)>(handler)(ok);
                }));
      };
      Base::StartWrite(&response, opts);
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), response);
  }

 private:
  void OnReadDone(bool ok) override {
    if (read_callback_) {
      auto f = std::move(read_callback_);
      f(ok);
    }
  }

  void OnWriteDone(bool ok) override {
    if (write_callback_) {
      auto f = std::move(write_callback_);
      f(ok);
    }
  }

 private:
  fu2::unique_function<void(bool ok)> read_callback_;
  fu2::unique_function<void(bool ok)> write_callback_;
};

template <typename Request,
          typename Response,
          template <typename, typename>
          typename Reactor>
class ServerRpcReactor : public RpcReactorBase<Request, Response, Reactor> {
 public:
  using Self = ServerRpcReactor<Request, Response, Reactor>;
  ServerRpcReactor(grpc::CallbackServerContext *server_context)
      : server_context_(server_context) {}

  static void operator delete(void *ptr) {
    RAY_LOG(DEBUG) << "ServerRPC delete";
    auto self = (Self *)(ptr);
    self->Finish(self->status_);
  }

 private:
  void OnDone() {
    RAY_LOG(DEBUG) << "ServerRPC OnDone";
    ::operator delete(this);
  }
  grpc::CallbackServerContext *server_context_;
  grpc::Status status_;
};

template <typename Request,
          typename Response,
          template <typename, typename>
          typename Reactor>
class ClientRpcReactor : public RpcReactorBase<Request, Response, Reactor> {
 public:
  using Base = RpcReactorBase<Request, Response, Reactor>;
  using Self = ClientRpcReactor<Request, Response, Reactor>;

  ClientRpcReactor() {}

  grpc::ClientContext &GetContext() { return client_context_; }

  void Start() {
    RAY_LOG(DEBUG) << "StartCall";
    Base::StartCall();
    Base::AddHold();
  }

  static void operator delete(void *ptr) { RAY_LOG(DEBUG) << "ClientRPC delete"; }

  // template<typename CompletionToken>
  // auto Finish(CompletionToken &&token) {

  // }
 private:
  void OnDone(const grpc::Status &status) override {
    RAY_LOG(DEBUG) << "ClientRPC OnDone";
    ::operator delete(this);
  }

  grpc::ClientContext client_context_;
};

}  // namespace rpc
}  // namespace ray
