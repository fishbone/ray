#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

#include <unordered_map>

#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"


namespace ray {
namespace rdma {
struct FabricContext;
class Fabric {
 public:
  Fabric() {}
  Fabric(const Fabric &) = delete;
  Fabric &operator=(const Fabric &) = delete;

  bool Init(const char* prov);
  const std::string& GetAddress() const;

  bool IsReady() const;

  bool AddPeer(const NodeID& node_id, const std::string& addr);

  std::optional<fi_addr_t> GetPeerAddr(const NodeID& node_id) {
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
      std::function<void(const ObjectID&)> push_done_cb,
      std::function<char*(const ObjectID&, size_t len)> prep_buf_cb,
      std::function<char*(const ObjectID&)> pull_done_cb) {
    push_done_ = push_done_cb;
    prepare_buf_ = prep_buf_cb;
    pull_done_ = pull_done_cb;
  }

  bool Push(const NodeID& node_id,
            const ObjectID& obj_id,
            const char* message,
            size_t len);

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
  std::unordered_map<NodeID, fi_addr_t> peers_;

  std::function<void(const ObjectID&)> push_done_;

  std::function<char*(const ObjectID&, size_t len)> prepare_buf_;
  std::function<char*(const ObjectID&)> pull_done_;

  std::unique_ptr<std::thread> pulling_;

};
}
}
