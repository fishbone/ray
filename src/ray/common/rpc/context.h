#pragma once

#include <grpcpp/server.h>

#include <boost/asio.hpp>
#include <queue>
#include <tuple>

#include "ray/common/rpc/function2.hpp"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

template <typename CompletionToken, typename Arg>
auto MakeCallback(CompletionToken &&handler) {
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

template <typename TRequest,
          typename TResponse,
          typename TReactor,
          typename TExecutor = boost::asio::system_executor>
struct RayReactor : public TReactor {
  using Request = TRequest;
  using Response = TResponse;
  using Reactor = TReactor;
  using Executor = TExecutor;

  RayReactor(Executor executor) : strand_(std::move(executor)) {
    if constexpr (!std::is_same_v<TExecutor, boost::asio::system_executor>) {
      RAY_CHECK(executor);
    }
  }

 protected:
  void Deref() {
    if (--ref_count_ == 0) {
      RAY_LOG(DEBUG) << "Destroying reactor";
      delete this;
    }
  }

  boost::asio::strand<Executor> strand_;
  // One ref for the shared ptr and the other ond for gRPC.
  std::atomic<int64_t> ref_count_{2};
};

template <typename TReactor>
class RayServerUnaryReactor : public TReactor {
 public:
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;
  using Executor = typename Reactor::Executor;

  RayServerUnaryReactor(grpc::CallbackServerContext *server_context, Executor executor)
      : Reactor(std::move(executor)), server_context_(server_context) {}
  virtual ~RayServerUnaryReactor() {
    RAY_LOG(DEBUG) << "RayServerUnaryReactor destroyed";
  }

  void Finish(const grpc::Status &status = grpc::Status::OK) {
    RAY_LOG(DEBUG) << "Server::Finish";
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
    RAY_LOG(DEBUG) << "ServerOnDone";
    this->Deref();
  }

  void OnCancel() override {
    RAY_LOG(DEBUG) << "Server:OnCancel";
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

template <typename BaseReactor, bool IsClient>
class RayReadReactor : virtual public BaseReactor {
 public:
  using Base = BaseReactor;
  using Reactor = typename Base::Reactor;
  using Request = typename Base::Request;
  using Response = typename Base::Response;
  using Executor = typename Base::Executor;
  using Data = std::conditional_t<IsClient, Response, Request>;

  template <typename... Args>
  RayReadReactor(Args &&...args) : Base(std::forward<Args>(args)...) {
    if constexpr (IsClient) {
      Base::AddHold();
    }
  }

  template <typename CompletionToken>
  auto Read(CompletionToken &&token) {
    auto init = [this](auto &&handler) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      auto f = MakeCallback<HandlerType, std::pair<Data, bool>>(
          std::forward<HandlerType>(handler));
      boost::asio::dispatch(this->strand_, [this, f = std::move(f)]() mutable {
        if (read_callbacks_.empty()) {
          read_callbacks_.push(std::move(f));
          this->StartRead(&data_);
        } else {
          read_callbacks_.push(std::move(f));
        }
      });
    };

    return boost::asio::async_initiate<CompletionToken, void(std::pair<Data, bool>)>(
        init, std::forward<CompletionToken>(token));
  }

 private:
  void OnReadDone(bool ok) override {
    boost::asio::dispatch(this->strand_, [this, ok]() mutable {
      RAY_CHECK(!read_callbacks_.empty());
      if (ok) {
        auto f = std::move(read_callbacks_.front());
        read_callbacks_.pop();
        f(std::make_pair(std::move(data_), ok));
        if (!read_callbacks_.empty()) {
          this->StartRead(&data_);
        }
      } else {
        while (!read_callbacks_.empty()) {
          auto f = std::move(read_callbacks_.front());
          read_callbacks_.pop();
          f(std::make_pair(Data(), false));
        }
        if constexpr (IsClient) {
          Base::RemoveHold();
        }
      }
    });
  }

  Data data_;
  std::queue<fu2::unique_function<void(std::pair<Data, bool>)>> read_callbacks_;
};

template <typename BaseReactor, bool IsClient>
class RayWriteReactor : virtual public BaseReactor {
 public:
  using Base = BaseReactor;
  using Reactor = typename Base::Reactor;
  using Request = typename Base::Request;
  using Response = typename Base::Response;
  using Executor = typename Base::Executor;
  using Data = std::conditional_t<IsClient, Request, Response>;

  template <typename... Args>
  RayWriteReactor(Args &&...args) : Base(std::forward<Args>(args)...) {
    if constexpr (IsClient) {
      Base::AddHold();
    }
  }

  template <typename CompletionToken>
  auto Write(const Data &data, CompletionToken &&token) {
    return Write(data, grpc::WriteOptions(), std::move(token));
  }

  template <typename CompletionToken>
  auto Write(const Data &data, grpc::WriteOptions opts, CompletionToken &&token) {
    auto init = [this, opts = std::move(opts)](auto &&handler, const auto &data) mutable {
      using HandlerType = std::decay_t<decltype(handler)>;
      auto f = MakeCallback<HandlerType, bool>(std::forward<HandlerType>(handler));
      boost::asio::dispatch(
          this->strand_, [this, f = std::move(f), &data, opts]() mutable {
            write_callbacks_.push(std::make_tuple(std::move(f), &data, opts));
            if (write_callbacks_.size() == 1) {
              opts.clear_buffer_hint();
              this->StartWrite(&data, opts);
            }
          });
    };

    return boost::asio::async_initiate<CompletionToken, void(bool)>(
        init, std::forward<CompletionToken>(token), data);
  }

 private:
  void OnWriteDone(bool ok) override {
    boost::asio::dispatch(this->strand_, [this, ok]() mutable {
      if (ok) {
        RAY_CHECK(!write_callbacks_.empty());
        auto f = std::get<0>(std::move(write_callbacks_.front()));
        write_callbacks_.pop();
        f(ok);
        if (!write_callbacks_.empty()) {
          auto &[_, data, opts] = write_callbacks_.front();
          if (write_callbacks_.size() == 1) {
            opts.clear_buffer_hint();
          }
          this->StartWrite(data, opts);
        }
      } else {
        while (!write_callbacks_.empty()) {
          auto f = std::get<0>(std::move(write_callbacks_.front()));
          f(false);
          write_callbacks_.pop();
        }

        if constexpr (IsClient) {
          Base::RemoveHold();
        }
      }
    });
  }

  std::queue<
      std::tuple<fu2::unique_function<void(bool)>, const Data *, grpc::WriteOptions>>
      write_callbacks_;
};

template <typename TReactor>
using RayServerReadReactor = RayReadReactor<RayServerUnaryReactor<TReactor>, false>;

template <typename TReactor>
using RayServerWriteReactor = RayWriteReactor<RayServerUnaryReactor<TReactor>, false>;

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
  using Executor = typename Base::Executor;

  RayServerBidiReactor(grpc::CallbackServerContext *server_context, Executor executor)
      : Base(server_context, executor),
        ReadBase(server_context, executor),
        WriteBase(server_context, executor) {}
};

template <typename TReactor>
class RayClientUnaryReactor : public TReactor {
 public:
  using Reactor = TReactor;
  using Request = typename Reactor::Request;
  using Response = typename Reactor::Response;
  using Executor = typename Reactor::Executor;

  RayClientUnaryReactor(Executor executor) : Reactor(std::move(executor)) {}

  virtual ~RayClientUnaryReactor() {
    RAY_LOG(DEBUG) << "RayClientUnaryReactor destroyed";
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
    RAY_LOG(DEBUG) << "hold_count_= " << hold_count_;
    Reactor::AddMultipleHolds(hold_count_);
    Reactor::StartCall();
  }

 protected:
  void AddHold() { ++hold_count_; }

  void RemoveHold() {
    --hold_count_;
    RAY_LOG(DEBUG) << "Client:RemoveHold";
    Reactor::RemoveHold();
  }

  void ClearHold() {
    while (hold_count_ > 0) {
      RemoveHold();
    }
  }

  void OnDone(const grpc::Status &status) override {
    RAY_LOG(DEBUG) << "ClientOnDone";
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
using RayClientWriteReactor = RayWriteReactor<RayClientUnaryReactor<TReactor>, true>;

template <typename TReactor>
using RayClientReadReactor = RayReadReactor<RayClientUnaryReactor<TReactor>, true>;

template <typename TReactor>
class RayClientBidiReactor : virtual public RayClientReadReactor<TReactor>,
                             virtual public RayClientWriteReactor<TReactor> {
 public:
  using ReadBase = RayClientReadReactor<TReactor>;
  using WriteBase = RayClientWriteReactor<TReactor>;
  using Base = typename ReadBase::Base;
  using Response = typename ReadBase::Response;
  using Request = typename WriteBase::Request;
  using Executor = typename Base::Executor;
  RayClientBidiReactor(Executor executor)
      : Base(executor), ReadBase(executor), WriteBase(executor) {}
};

template <typename Reactor>
std::shared_ptr<Reactor> MakeServerReactor(grpc::CallbackServerContext *context) {
  return std::shared_ptr<Reactor>(new Reactor(context, {}), [](auto p) {
    p->Finish();
    RAY_LOG(DEBUG) << "Server::Deref";
    p->Deref();
  });
}

template <typename Reactor>
std::shared_ptr<Reactor> MakeClientReactor() {
  return std::shared_ptr<Reactor>(new Reactor({}), [](auto p) {
    RAY_LOG(DEBUG) << "Client::Deref";
    p->ClearHold();
    p->Deref();
  });
}

}  // namespace rpc
}  // namespace ray
