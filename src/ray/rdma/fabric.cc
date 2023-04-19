#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>

#include "ray/rdma/fabric.h"
#include "ray/util/logging.h"


#define RAY_RDMA_INIT(OP, ...)                                          \
  do {                                                                  \
  int err = OP(__VA_ARGS__);                                            \
  if (err) {                                                            \
    RAY_LOG(ERROR) << "Init RDMA failed: " #OP << " " << strerror(-err); \
    return false;                                                       \
  }                                                                     \
  }                                                                     \
  while(false)

namespace ray {
namespace rdma {

/*
  START -> KEY EXCHANGED -> FINISHED
 */

enum struct Tag : uint64_t {
  PULL_REQ = 0,
  PULL_DONE = 1
};

struct FabricContext {
  void ResetBuff() {
    buff.resize(1024 * 2);
  }

  rpc::FabricPushMeta meta;
  fi_addr_t peer;
  fid_mr* mr;
  std::function<void()> done_cb;
  std::string buff;
};


bool Fabric::Init(const char* prov) {
  RAY_LOG(INFO) << "Initialize RDMA with " << prov;
  auto version = FI_VERSION(1, 18);
  auto hints = fi_allocinfo();
  if(!hints) {
    RAY_LOG(ERROR) << "Init hints failed: ";
    return false;
  }

  hints->fabric_attr->prov_name = strdup(prov);
  hints->ep_attr->type = FI_EP_RDM;
  hints->caps = FI_MSG | FI_RMA | FI_REMOTE_COMM | FI_LOCAL_COMM;
  hints->domain_attr->mr_mode = FI_RMA;
  RAY_RDMA_INIT(fi_getinfo, version, NULL, NULL, 0, hints, &fi_);
  fi_freeinfo(hints);
  RAY_RDMA_INIT(fi_fabric, fi_->fabric_attr, &fabric_, NULL);
  RAY_RDMA_INIT(fi_domain, fabric_, fi_, &domain_, NULL);
  RAY_RDMA_INIT(fi_endpoint, domain_, fi_, &ep_, NULL);
  fi_av_attr av_attr;
  std::memset(&av_attr, 0, sizeof(av_attr));
  av_attr.type = FI_AV_TABLE;
  RAY_RDMA_INIT(fi_av_open, domain_, &av_attr, &av_, NULL);
  RAY_RDMA_INIT(fi_ep_bind, ep_, &av_->fid, 0);
  fi_cq_attr cq_attr;
  std::memset(&cq_attr, 0, sizeof(cq_attr));
  cq_attr.format = FI_CQ_FORMAT_MSG;
  cq_attr.size = 1024 * 1024;
  RAY_RDMA_INIT(fi_cq_open, domain_, &cq_attr, &cq_, NULL);
  RAY_RDMA_INIT(fi_ep_bind, ep_, &cq_->fid, FI_RECV | FI_SEND);
  RAY_RDMA_INIT(fi_enable, ep_);

  size_t len = 64;
  src_addr_.resize(len);
  RAY_RDMA_INIT(fi_getname, &ep_->fid, (char *)src_addr_.data(), &len);
  RAY_CHECK(len <= src_addr_.size());
  src_addr_.resize(len);
  ready_ = true;
  RAY_LOG(INFO) << "RMA Init successfully:\n" << fi_tostr(fi_, FI_TYPE_INFO);
  return true;
}

const std::string& Fabric::GetAddress() const {
  return src_addr_;
}

bool Fabric::IsReady() const {
  return ready_;
}

bool Fabric::AddPeer(const std::string& node_id, const std::string& addr) {
  RAY_CHECK(peers_.find(node_id) == peers_.end());
  fi_addr_t fi_addr;
  memset(&fi_addr, 0, sizeof(fi_addr));
  if(fi_av_insert(av_, &addr, 1, &fi_addr, 0, NULL) != 1) {
    RAY_LOG(ERROR) << "Failed to rdma address for node: " << node_id;
    return false;
  }
  peers_[node_id] = fi_addr;

  return true;
}

void Fabric::InitWait(fi_addr_t fi_addr) {
  auto cxt = new FabricContext();
  cxt->ResetBuff();
  cxt->peer = fi_addr;
  RAY_CHECK(fi_trecv(ep_, (char*)cxt->buff.c_str(), cxt->buff.size(), NULL, fi_addr, (uint64_t)Tag::PULL_REQ, 0, cxt) != 0);
}

bool Fabric::Push(rpc::FabricPushMeta meta,
                  std::function<void()> cb) {
  const auto& node_id = meta.node_id();
  auto iter = peers_.find(node_id);
  if(iter == peers_.end()) {
    return false;
  }

  auto cxt = new FabricContext();
  cxt->done_cb = cb;

  // Register MR
  RAY_CHECK(fi_mr_reg(domain_,
                      (void*)meta.mem_addr(),
                      meta.metadata_size() + meta.data_size(),
                      FI_REMOTE_READ,
                      0,
                      0,
                      0,
                      &cxt->mr, NULL) == 0);
  cxt->meta = std::move(meta);
  RAY_CHECK(cxt->meta.SerializeToString(&cxt->buff));
  cxt->meta.set_mem_key(fi_mr_key(cxt->mr));

  cxt->peer = iter->second;
  RAY_CHECK(fi_tsend(ep_, (char*)cxt->buff.c_str(), cxt->buff.size(), NULL, cxt->peer, (uint64_t)Tag::PULL_REQ, cxt) == 0);

  return true;
}

void Fabric::Start() {
  pulling_ = std::make_unique<std::thread>([this] {
    const size_t MAX_POLL_CNT = 10;
    fi_cq_tagged_entry comps[MAX_POLL_CNT];
    do {
      auto ret = fi_cq_read(cq_, &comps, MAX_POLL_CNT);
      if(ret == 0 || ret == -FI_EAGAIN) {
        continue;
      }

      RAY_CHECK(ret >= 0);
      for(auto i = 0; i < ret; ++i) {
        auto& comp = comps[i];
        auto cxt = (FabricContext*)comp.op_context;
        RAY_CHECK(cxt);
        if(comp.flags & FI_RMA && comp.flags & FI_READ) {
          RAY_CHECK(!fi_tsend(ep_, NULL, 0, NULL, cxt->peer, (uint64_t)Tag::PULL_DONE, cxt));
        } else if(comp.flags & FI_TAGGED && comp.flags & FI_SEND) {
          switch((Tag)comp.tag) {
            case Tag::PULL_REQ:
              cxt->ResetBuff();
              RAY_CHECK(!fi_trecv(ep_, (char*)cxt->buff.c_str(), cxt->buff.size(), NULL, cxt->peer, (uint64_t)Tag::PULL_DONE, 0, cxt));
              break;
            case Tag::PULL_DONE:
              delete cxt;
              break;
            default:
              RAY_LOG(FATAL) << "Unknown tag: " << comp.tag;
          }
        } else if (comp.flags & FI_TAGGED && comp.flags & FI_RECV) {
          switch((Tag)comp.tag) {
            case Tag::PULL_REQ: {
              InitWait(cxt->peer);
              cxt->buff.resize(comp.len);
              io_context_.post([this, cxt] {
                cxt->meta.ParseFromString(cxt->buff);
                auto [data, len] = prepare_buf_(cxt->meta);
                RAY_CHECK(fi_read(ep_, data, len, NULL, cxt->peer, cxt->meta.mem_addr(), cxt->meta.mem_key(), cxt) == 0);
              });
              break;
            }
            case Tag::PULL_DONE:
              PushDone(cxt);
              break;
            default:
              RAY_LOG(FATAL) << "Unknown tag: " << comp.tag;
          }
        } else {
          RAY_LOG(FATAL) << "Unprocessed tag: " << comp.tag << " or flags: " << comp.flags;
        }
      }
    } while(true);
  });
}

void Fabric::PushDone(FabricContext* cxt) {
  // Close mr
  fi_close(&cxt->mr->fid);
  if(cxt->done_cb) {
    io_context_.post(cxt->done_cb);
  }
  delete cxt;
}

Fabric::~Fabric() {
  if(ep_) {
    fi_close(&ep_->fid);
  }
  if(cq_) {
    fi_close(&cq_->fid);
  }
  if(av_) {
    fi_close(&av_->fid);
  }
  if(domain_) {
    fi_close(&domain_->fid);
  }
  if(fabric_) {
    fi_close(&fabric_->fid);
  }
  if(fi_) {
    fi_freeinfo(fi_);
  }
}


}
}
