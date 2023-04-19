#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

#include <unordered_map>
#include "boost/asio.hpp"

#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"


namespace ray {
namespace rdma {
struct FabricContext;
class Fabric {
 public:
  Fabric(boost::asio::io_context& io_context)
      : io_context_(io_context) {}
  Fabric(const Fabric &) = delete;
  Fabric &operator=(const Fabric &) = delete;

  bool Init(const char* prov);
  const std::string& GetAddress() const;

  bool IsReady() const;

  bool AddPeer(const std::string& node_id, const std::string& addr);

  std::optional<fi_addr_t> GetPeerAddr(const std::string& node_id) {
    auto iter =  peers_.find(node_id);
    if(iter == peers_.end()) {
      return std::nullopt;
    }
    return iter->second;
  }

  ~Fabric();

  void Start();

  // TODO: Better API
  void SetCB(
      std::function<void(const ObjectID&)> pull_done,
      std::function<std::pair<char*, size_t>(const rpc::FabricPushMeta&)> prep_buf) {
    prepare_buf_ = prep_buf;
    pull_done_ = pull_done;
  }

  bool Push(rpc::FabricPushMeta meta, std::function<void()> cb);

 private:

  void PushDone(FabricContext* cxt);

  void InitWait(fi_addr_t fi_addr);

  bool ready_ = false;
  std::string src_addr_;
  fi_info *fi_ = nullptr;
  fid_fabric *fabric_ = nullptr;
  fid_domain *domain_ = nullptr;
  fid_av *av_ = nullptr;
  fid_ep *ep_ = nullptr;
  std::string ep_addr_;
  fid_cq *cq_ = nullptr;
  std::unordered_map<std::string, fi_addr_t> peers_;

  std::function<std::pair<char*, size_t>(const rpc::FabricPushMeta&)> prepare_buf_;

  std::function<void(const ObjectID&)> pull_done_;

  std::unique_ptr<std::thread> pulling_;
  boost::asio::io_context& io_context_;
};
}
}
