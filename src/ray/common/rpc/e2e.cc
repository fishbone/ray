#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include "context.h"
#include "src/ray/protobuf/test_service.grpc.pb.h"
using namespace ray;
using namespace ray::rpc;

boost::asio::io_context io_context;

class TestStreamingServiceImpl final : public TestStreamingService::CallbackService {
 public:
  grpc::ServerBidiReactor<PingRequest, PingReply> *Ping(
      grpc::CallbackServerContext *context) override {
    RAY_LOG(INFO) << "Server: Ping request";
    auto reactor = std::make_shared<
        ServerRpcReactor<PingRequest, PingReply, grpc::ServerBidiReactor>>(context);
    auto run = [](auto reactor) -> boost::asio::awaitable<void> {
      PingRequest request;
      while (co_await reactor->Read(request, boost::asio::use_awaitable)) {
        RAY_LOG(INFO) << "ServerRead: " << request.cnt()  << " " << std::this_thread::get_id();
        PingReply reply;
        reply.set_cnt(request.cnt());
        if (!co_await reactor->Write(reply, boost::asio::use_awaitable)) {
          break;
        }
        RAY_LOG(INFO) << "ServerWrite: " << reply.cnt()  << " " << std::this_thread::get_id();
      }
      RAY_LOG(INFO) << "Server: EOF" << " " << std::this_thread::get_id();
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
    RAY_LOG(INFO) << "Create channel";
    auto channel =
        grpc::CreateChannel("127.0.0.1:8090", grpc::InsecureChannelCredentials());
    auto stub = TestStreamingService::NewStub(channel);
    auto reactor = std::make_shared<
        ClientRpcReactor<PingRequest, PingReply, grpc::ClientBidiReactor>>();

    stub->async()->Ping(&reactor->GetContext(), reactor.get());
    reactor->Start();
    auto run = [](auto reactor) -> boost::asio::awaitable<void> {
      int64_t i = 0;
      PingRequest request;
      request.set_cnt(i++);
      PingReply reply;
      while (co_await reactor->Write(request, boost::asio::use_awaitable)) {
        RAY_LOG(INFO) << "ClientWrite: " << request.cnt() << " " << std::this_thread::get_id();
        if (!co_await reactor->Read(reply, boost::asio::use_awaitable)) {
          RAY_LOG(INFO) << "ClientRead: EOF"  << " " << std::this_thread::get_id();
          break;
        }
        RAY_LOG(INFO) << "ClientRead: " << reply.cnt()  << " " << std::this_thread::get_id();
        request.set_cnt(i++);
      }
      RAY_LOG(INFO) << "Client: EOF";
    };
    boost::asio::co_spawn(io_context.get_executor(), run(reactor), boost::asio::detached);
  });
  return t;
}

int main() {
  auto server = StartServer();
  auto client = StartClient();
  auto t = std::make_unique<std::thread>([]() {
    boost::asio::io_context::work work(io_context);
    io_context.run();
  });

  t->join();
  server->join();
  client->join();
  return 0;
}
