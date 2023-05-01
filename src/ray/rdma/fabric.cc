#include "ray/rdma/fabric.h"

#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>

#include <functional>
#include <iostream>

#include "ray/util/logging.h"
size_t kBuffSize = 4096;

#define RAY_RDMA_ACTION(OP, ...)                                   \
  RAY_LOG(INFO) << "RDMA:" << #OP;                                 \
  do {                                                             \
    int err = OP(__VA_ARGS__);                                     \
    if (err == -FI_EAGAIN) {                                       \
      continue;                                                    \
    }                                                              \
    RAY_CHECK(err == 0) << #OP << " failed: " << fi_strerror(err); \
    break;                                                         \
  } while (true)

#define RAY_RDMA_INIT(OP, ...)                                             \
  do {                                                                     \
    int err = OP(__VA_ARGS__);                                             \
    if (err) {                                                             \
      RAY_LOG(ERROR) << "Init RDMA failed: " #OP << " " << strerror(-err); \
      return false;                                                        \
    }                                                                      \
  } while (false)

namespace ray {
namespace rdma {

struct Context {
  std::function<void()> cb;
};

bool Fabric::Init(const char *prov) {
  RAY_LOG(INFO) << "Initialize RDMA with " << prov;
  auto version = FI_VERSION(1, 18);
  auto hints = fi_allocinfo();
  if (!hints) {
    RAY_LOG(ERROR) << "Init hints failed: ";
    return false;
  }
  hints->fabric_attr->prov_name = strdup(prov);
  hints->ep_attr->type = FI_EP_RDM;
  hints->caps = FI_MSG | FI_RMA | FI_REMOTE_COMM | FI_LOCAL_COMM;
  hints->domain_attr->mr_mode = FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;
  // hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
  hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
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

const std::string &Fabric::GetAddress() const { return src_addr_; }

bool Fabric::IsReady() const { return ready_; }

std::tuple<uint64_t, int64_t, void *> Fabric::RegisterMemory(const char *mem, size_t s) {
  fid_mr *mr = nullptr;
  int ret = fi_mr_reg(domain_, (void *)mem, s, FI_REMOTE_READ, 0, 0, 0, &mr, NULL);
  RAY_CHECK(ret == 0) << "Failed fi_mr_reg: " << fi_strerror(ret) << ". " << (void *)mem
                      << " " << s;

  return std::make_tuple((uint64_t)mem, fi_mr_key(mr), (void *)mr);
}

void Fabric::Read(const std::string &dest,
                  char *buff,
                  size_t len,
                  uint64_t mem_addr,
                  uint64_t mem_key,
                  std::function<void()> cb) {
  auto cxt = new Context();
  cxt->cb = std::move(cb);

  RAY_CHECK(peers_.find(dest) != peers_.end());
  auto peer_addr = peers_[dest];
  int err = 0;
  do {
    err = fi_read(ep_, buff, len, NULL, peer_addr, mem_addr, mem_key, cxt);
    RAY_CHECK(err == 0 || err == -EAGAIN);
  } while (err == -EAGAIN);
}

bool Fabric::AddPeer(const std::string &node_id, const std::string &addr) {
  RAY_CHECK(peers_.find(node_id) == peers_.end());
  fi_addr_t fi_addr;
  memset(&fi_addr, 0, sizeof(fi_addr));
  char buff[1024];
  size_t len = sizeof(buff);
  RAY_LOG(INFO) << "Adding peer " << fi_av_straddr(av_, addr.c_str(), buff, &len);
  if (fi_av_insert(av_, addr.c_str(), 1, &fi_addr, 0, NULL) != 1) {
    RAY_LOG(ERROR) << "Failed to rdma address for node: " << node_id;
    return false;
  }
  peers_[node_id] = fi_addr;
  return true;
}

void Fabric::Start() {
  pulling_ = std::make_unique<std::thread>([this] {
    const size_t MAX_POLL_CNT = 10;
    fi_cq_msg_entry comps[MAX_POLL_CNT];
    do {
      auto ret = fi_cq_read(cq_, comps, MAX_POLL_CNT);
      if (ret == 0 || ret == -FI_EAGAIN) {
        continue;
      }

      RAY_CHECK(ret > 0) << fi_strerror(ret) << ", error_code=" << ret;

      for (int i = 0; i < ret; ++i) {
        auto &comp = comps[i];
        if (comp.op_context) {
          auto *cxt = (Context *)comp.op_context;
          io_context_.post(std::move(std::move(cxt->cb)));
          delete cxt;
        }
      }
    } while (ready_);
  });
}

Fabric::~Fabric() {
  ready_ = false;
  if (pulling_) {
    pulling_->join();
  }
  if (ep_) {
    fi_close(&ep_->fid);
  }
  if (cq_) {
    fi_close(&cq_->fid);
  }
  if (av_) {
    fi_close(&av_->fid);
  }
  if (domain_) {
    fi_close(&domain_->fid);
  }
  if (fabric_) {
    fi_close(&fabric_->fid);
  }
  if (fi_) {
    fi_freeinfo(fi_);
  }
}

}  // namespace rdma
}  // namespace ray
