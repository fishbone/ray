#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <iostream>

#include "context.h"
#include "src/ray/protobuf/test_service.grpc.pb.h"
using namespace ray;
using namespace ray::rpc;

boost::asio::io_context io_context;
size_t cnt = 0;
class TestStreamingServiceImpl final : public TestStreamingService::CallbackService {
 public:
  grpc::ServerBidiReactor<PingRequest, PingReply> *Ping(
      grpc::CallbackServerContext *context) override {
    using Reactor =
        RayServerBidiReactor<RayReactor<PingRequest,
                                        PingReply,
                                        grpc::ServerBidiReactor<PingRequest, PingReply>>>;
    RAY_LOG(ERROR) << "Server: Ping request";
    auto reactor = MakeServerReactor<Reactor>(context);
    auto run = [](auto reactor) -> boost::asio::awaitable<void> {
      for (auto [request, status] = co_await reactor->Read(boost::asio::use_awaitable);
           status;
           std::tie(request, status) =
               co_await reactor->Read(boost::asio::use_awaitable)) {
        RAY_LOG(ERROR) << "ServerRead: " << request.cnt() << " "
                       << std::this_thread::get_id();
        PingReply reply;
        reply.set_cnt(request.cnt());
        if (auto status = co_await reactor->Write(reply, boost::asio::use_awaitable);
            !status) {
          break;
        }
        RAY_LOG(ERROR) << "ServerWrite: " << reply.cnt() << " "
                       << std::this_thread::get_id();
      }
      RAY_LOG(ERROR) << "EOF";
    };
    boost::asio::co_spawn(io_context.get_executor(), run(reactor), boost::asio::detached);
    return reactor.get();
  }
};

std::unique_ptr<std::thread> StartServer() {
  auto t = std::make_unique<std::thread>([]() {
    TestStreamingServiceImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:8090", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    server->Wait();
  });
  return t;
}

std::unique_ptr<std::thread> StartClient() {
  auto t = std::make_unique<std::thread>([]() {
    RAY_LOG(ERROR) << "Create channel";
    auto channel =
        grpc::CreateChannel("127.0.0.1:8090", grpc::InsecureChannelCredentials());
    auto stub = TestStreamingService::NewStub(channel);

    auto reactor = MakeClientReactor<RayClientBidiReactor<
        RayReactor<PingRequest,
                   PingReply,
                   grpc::ClientBidiReactor<PingRequest, PingReply>>>>();

    RAY_LOG(ERROR) << "Client:??";
    stub->async()->Ping(&reactor->GetContext(), reactor.get());
    RAY_LOG(ERROR) << "Client:Pinged";
    reactor->StartCall();
    RAY_LOG(ERROR) << "Client: StartCall";
    auto run = [](auto reactor) -> boost::asio::awaitable<void> {
      int64_t i = 0;
      PingRequest request;
      request.set_cnt(i++);
      PingReply reply;
      auto status1 = co_await reactor->Write(request, boost::asio::use_awaitable);
      auto status2 = co_await reactor->Write(request, boost::asio::use_awaitable);
      while (status1 && status2) {
        RAY_LOG(ERROR) << "ClientWrite: " << request.cnt() << " "
                       << std::this_thread::get_id();
        std::tie(reply, status1) = co_await reactor->Read(boost::asio::use_awaitable);
        std::tie(reply, status2) = co_await reactor->Read(boost::asio::use_awaitable);
        if (!status1 || !status2) {
          RAY_LOG(ERROR) << "ClientRead: EOF"
                         << " " << std::this_thread::get_id();
          break;
        }
        RAY_LOG(ERROR) << "ClientRead: " << reply.cnt() << " "
                       << std::this_thread::get_id();
        request.set_cnt(i++);
        status1 = co_await reactor->Write(request, boost::asio::use_awaitable);
        status2 = co_await reactor->Write(request, boost::asio::use_awaitable);
      }
      RAY_LOG(ERROR) << "Client: EOF:";
    };

    auto cancel = [](auto reactor) -> boost::asio::awaitable<void> {
      boost::asio::steady_timer timer(io_context);
      timer.expires_after(std::chrono::seconds(1));
      co_await timer.async_wait(boost::asio::use_awaitable);
      reactor->GetContext().TryCancel();
      RAY_LOG(ERROR) << "Cancel Called";
    };
    boost::asio::co_spawn(
        io_context.get_executor(), cancel(reactor), boost::asio::detached);
    boost::asio::co_spawn(io_context.get_executor(), run(reactor), boost::asio::detached);
  });
  return t;
}

int main() {
  auto server = StartServer();
  std::cout << "SERVER" << std::endl;
  auto client = StartClient();
  std::cout << "CLIENT" << std::endl;
  auto t = std::make_unique<std::thread>([]() {
    boost::asio::io_context::work work(io_context);
    io_context.run();
  });

  std::cout << "INITED" << std::endl;
  client->join();
  std::cout << "Client joined" << std::endl;
  server->join();
  t->join();
  return 0;
}
