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
  Fabric(boost::asio::io_context &io_context) : io_context_(io_context) {}
  Fabric(const Fabric &) = delete;
  Fabric &operator=(const Fabric &) = delete;

  bool Init(const char *prov);
  const std::string &GetAddress() const;

  bool IsReady() const;

  bool AddPeer(const std::string &node_id, const std::string &addr);

  std::optional<fi_addr_t> GetPeerAddr(const std::string &node_id) {
    auto iter = peers_.find(node_id);
    if (iter == peers_.end()) {
      return std::nullopt;
    }
    return iter->second;
  }

  ~Fabric();

  void Start();

  std::tuple<uint64_t, int64_t, void*> RegisterMemory(const char* mem, size_t s);
  void Read(const std::string& dest,
            char* buff,
            size_t len,
            uint64_t mem_addr,
            uint64_t mem_key,
            std::function<void()> cb);
 private:
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

  std::unique_ptr<std::thread> pulling_;
  boost::asio::io_context &io_context_;
};
}  // namespace rdma
}  // namespace ray
