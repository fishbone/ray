#include <grpcpp/server.h>

#include <boost/asio.hpp>

#include "ray/common/rpc/function2.hpp"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

template <typename Request,
          typename Response,
          template <typename, typename>
          typename Reactor>
class RpcReactorBase : public Reactor<Request, Response> {
  using Base = Reactor<Request, Response>;

 public:
  template <typename CompletionToken>
  auto Read(Request &request, CompletionToken &&token) {
    auto init = [this](auto &&handler, Request *request) mutable {
      RAY_CHECK(!read_callback_);
      Base::StartRead(request);
      read_callback_ = [handler =
                            std::forward<decltype(handler)>(handler)](bool ok) mutable {
        auto alloc = boost::asio::get_associated_allocator(
            handler, boost::asio::recycling_allocator<void>());
        auto ex = boost::asio::get_associated_executor(handler);
        // RAY_CHECK(ex) << "Invalid executor";
        boost::asio::dispatch(
            ex,
            boost::asio::bind_allocator(
                alloc,
                [handler = std::forward<decltype(handler)>(handler), ok]() mutable {
                  std::forward<decltype(handler)>(handler)(ok);
                }));
      };
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), &request);
  }

  template <typename CompletionToken>
  auto Write(const Response &response, CompletionToken &&token) {
    return Write(response, grpc::WriteOptions(), std::move(token));
  }

  template <typename CompletionToken>
  auto Write(const Response &response, grpc::WriteOptions opts, CompletionToken &&token) {
    auto init = [this, opts = std::move(opts)](auto &&handler,
                                               const Response &response) mutable {
      RAY_CHECK(!write_callback_);
      Base::StartWrite(&response, opts);

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
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), response);
  }

 private:
  void OnReadDone(bool ok) override {
    if (read_callback_) {
      std::move(read_callback_)(ok);
    }
  }

  void OnWriteDone(bool ok) override {
    if (write_callback_) {
      std::move(write_callback_)(ok);
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
class ClientRpcReactor : private RpcReactorBase<Request, Response, Reactor> {
  using Base = RpcReactorBase<Request, Response, Reactor>;

 public:
  grpc::ClientContext &GetContext() { return client_context_; }

  void Start() { Base::StartCall(); }

 private:
  void OnDone(const grpc::Status &) override { delete this; }
  grpc::ClientContext client_context_;
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
    auto self = (Self *)(ptr);
    self->Finish(self->status_);
  }

 private:
  void OnDone() { ::operator delete(this); }
  grpc::CallbackServerContext *server_context_;
  grpc::Status status_;
};

}  // namespace rpc
}  // namespace ray
