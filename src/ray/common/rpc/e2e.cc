#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <grpcpp/grpcpp.h>
#include "src/ray/protobuf/test_service.grpc.pb.h"
#include "context.h"
using namespace ray;
using namespace ray::rpc;

boost::asio::io_context io_context;

class TestStreamingServiceImpl final : public TestStreamingService::CallbackService {
 public:
  grpc::ServerBidiReactor<PingRequest, PingReply> *Ping(
      grpc::CallbackServerContext *context) override {
    auto reactor = std::make_shared<ServerRpcReactor<PingRequest, PingReply, grpc::ServerBidiReactor>>(context);
    auto run = [reactor]() -> boost::asio::awaitable<void> {
      PingRequest request;
      while (co_await reactor->Read(request, boost::asio::use_awaitable)) {
        PingReply reply;
        if(!co_await reactor->Write(reply, boost::asio::use_awaitable)) {
          break;
        }
      }
    };

    boost::asio::co_spawn(
        io_context.get_executor(),
        run(),
        boost::asio::detached);
    return reactor.get();
  }
 private:
};

std::unique_ptr<std::thread> StartServer() {
  auto t = std::make_unique<std::thread>([]() {
    TestStreamingServiceImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:8090", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    server->Wait();
  });
  return t;
}

int main() {
  auto server = StartServer();
  auto t = std::make_unique<std::thread>([]() {
    boost::asio::io_context::work work(io_context);
    io_context.run();
  });
  t->join();
  return 0;
}
