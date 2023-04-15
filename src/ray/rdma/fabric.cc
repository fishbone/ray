#include <memory>
#include <mutex>
#include <boost/asio.hpp>
#include <string_view>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_errno.h>
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

using boost::asio::ip::tcp;
#define MR_KEY 0xABCD
namespace ray {
namespace rdma {

#define CHK_ERR(name, cond, err)                            \
  do {                                                      \
    if (cond) {                                             \
      fprintf(stderr,"%s: %s\n", name, strerror(-(err)));   \
      exit(1);                                              \
    }                                                       \
  } while (0)


inline std::ostream& operator<<(std::ostream& os, const fi_info* i) {
  static std::mutex mutex;
  std::lock_guard<std::mutex> _(mutex);
  os << fi_tostr(i, FI_TYPE_INFO);
  return os;
}

inline void crash(const char* msg) {
  perror(msg);
  std::exit(1);
}

class Fabric {
 public:
  Fabric(){}
  Fabric(const Fabric&) = delete;
  Fabric& operator=(const Fabric&) = delete;

  void StartAsServer(int port) {
    thread_ = std::make_unique<std::thread>([=]{
      tcp::acceptor acceptor(io_context_, tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), port));
      for (;;) {
        tcp::socket socket(io_context_);
        acceptor.accept(socket);
        Exchange(socket);
      }
    });
  }

  void Exchange(tcp::socket& socket) {
    char buff[64];
    size_t len = sizeof(buff);
    using namespace boost;
    system::error_code ignored_error;
    asio::write(socket, boost::asio::buffer(ep_addr_), ignored_error);
    len = asio::read(
        socket, asio::buffer(buff, len),
        asio::transfer_exactly(ep_addr_.size()));
    AddPeer(buff);

    asio::write(socket, asio::buffer(&local_write_key_, sizeof(int64_t)), ignored_error);
    asio::read(socket, asio::buffer(&remote_write_key_, sizeof(int64_t)), asio::transfer_exactly(sizeof(int64_t)));

    asio::write(socket, asio::buffer(&local_write_addr_, sizeof(void*)), ignored_error);
    asio::read(socket, asio::buffer(&remote_write_addr_, sizeof(void*)), asio::transfer_exactly(sizeof(void*)));


    asio::write(socket, asio::buffer(&local_read_key_, sizeof(int64_t)), ignored_error);
    asio::read(socket, asio::buffer(&remote_read_key_, sizeof(int64_t)), asio::transfer_exactly(sizeof(int64_t)));

    asio::write(socket, asio::buffer(&local_read_addr_, sizeof(void*)), ignored_error);
    asio::read(socket, asio::buffer(&remote_read_addr_, sizeof(void*)), asio::transfer_exactly(sizeof(void*)));

    ShowPeers();

  }

  void ConnectToServer(int port) {
    auto endpoint = tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), port);
    tcp::resolver resolver(io_context_);

    tcp::socket socket(io_context_);
    boost::asio::connect(socket, resolver.resolve(endpoint));
    Exchange(socket);
  }

  void ShowPeers() {
    std::cout << fi_tostr(fi_, FI_TYPE_INFO) << std::endl;
    for(auto& addr : peers_) {
      char buf[64];
      size_t len = sizeof(buf);
      if(fi_av_lookup(av_, addr, buf, &len)) {
        crash("fi_av_lookup");
      }
      char str[128];
      size_t len_str = 128;
      fi_av_straddr(av_, buf, str, &len_str);
      std::cout << "ShowPeers: " << str << std::endl;
    }

    std::cout << "RemoteWriteKey: " << remote_write_key_ << std::endl;
    std::cout << "RemoteWriteAddr: " << remote_write_addr_ << std::endl;

    std::cout << "RemoteReadKey: " << remote_read_key_ << std::endl;
    std::cout << "RemoteReadAddr: " << remote_read_addr_ << std::endl;

    std::cout << "LocalWriteKey: " << local_write_key_ << std::endl;
    std::cout << "LocalWriteAddr: " << (uint64_t)write_buff_ << std::endl;
    std::cout << "LocalWriteDesc: " << write_desc_ << std::endl;

    std::cout << "LocalReadKey: " << local_read_key_ << std::endl;
    std::cout << "LocalReadAddr: " << (uint64_t)read_buff_ << std::endl;
    std::cout << "LocalReadDesc: " << read_desc_ << std::endl;

  }

  fi_addr_t AddPeer(const char* addr) {
    fi_addr_t fi_addr;
    memset(&fi_addr, 0, sizeof(fi_addr));
    if(fi_av_insert(av_, addr, 1, &fi_addr, 0, NULL) != 1) {
      std::cout << "av_insert error" << std::endl;
      std::exit(1);
    }
    peers_.push_back(fi_addr);
    return fi_addr;
  }

  void InitInfo(const char* prov_name,
                enum fi_ep_type ep_type,
                uint64_t caps,
                int mr_mode = FI_MR_UNSPEC) {
    auto hints = fi_allocinfo();
    auto version = FI_VERSION(1, 18);
    hints->ep_attr->type = ep_type;
    hints->caps = FI_MSG | FI_RMA;
    hints->fabric_attr->prov_name = strdup(prov_name);
    hints->tx_attr->op_flags = FI_DELIVERY_COMPLETE;
    hints->domain_attr->mr_mode = mr_mode;
    if(fi_getinfo(version, NULL, NULL, 0, hints, &fi_)) {
      crash("fi_getinfo");
    }

    fi_freeinfo(hints);
  }

  void InitFabric() {
    if(fi_fabric(fi_->fabric_attr, &fabric_, NULL)) {
      crash("fi_fabric");
    }
  }

  void InitDomain() {
    if(fi_domain(fabric_, fi_, &domain_, NULL)) {
      crash("fi_domain failed");
    }
  }

  void InitAV(fi_av_type av_type = FI_AV_UNSPEC) {
    fi_av_attr av_attr;
    std::memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type = av_type;
    if(fi_av_open(domain_, &av_attr, &av_, NULL)) {
      crash("fi_av_open");
    }

    if(fi_ep_bind(ep_, &av_->fid, 0)) {
      crash("fi_ep_bind:av");
    }
  }

  void InitCQ(size_t size,
              enum fi_cq_format cq_format = FI_CQ_FORMAT_UNSPEC) {
    fi_cq_attr cq_attr;
    std::memset(&cq_attr, 0, sizeof(cq_attr));
	cq_attr.format = cq_format;
	cq_attr.size = size;
    if(fi_cq_open(domain_, &cq_attr, &cq_, NULL)) {
      crash("fi_cq_open");
    }

    if(fi_ep_bind(ep_, &cq_->fid, FI_SEND|FI_RECV)) {
      crash("fi_ep_bind:cq");
    }
  }

  void Finish() {
    if(fi_enable(ep_)) {
      crash("fi_enable");
    }

    size_t len = 64;
    ep_addr_.resize(len);

    if(fi_getname(&ep_->fid, (char*)ep_addr_.data(), &len)) {
      crash("fi_getname");
    }
    ep_addr_.resize(len);
    AddPeer(ep_addr_.data());
  }

  const char* GetWriteBuffer() {
    return write_buff_;
  }

  const char* GetReadBuffer() {
    return read_buff_;
  }

  std::string GetEpAddress() {
    char buff[64];
    size_t len = 64;
    fi_av_straddr(av_, ep_addr_.data(), buff, &len);
    return std::string(buff, len);
  }

  void InitEndpoint() {
    if(fi_endpoint(domain_, fi_, &ep_, NULL)) {
      crash("fi_endpoint");
    }
  }

  void RegisterMR(size_t len) {
    write_buff_ = (char*)calloc(len, 1);
    read_buff_ = (char*)calloc(len, 1);

    if(fi_mr_reg(domain_, write_buff_, len, FI_WRITE | FI_REMOTE_WRITE, 0, MR_KEY, 0, &write_mr_, NULL)) {
      crash("RegistMR-W failed");
    }

    bool vitr_addr = false;
    if(fi_->domain_attr->mr_mode == FI_MR_UNSPEC || (fi_->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
      vitr_addr = true;
    }

    write_desc_ = fi_mr_desc(write_mr_);
    local_write_key_ = fi_mr_key(write_mr_);
    local_write_addr_ = vitr_addr ? (uint64_t)write_buff_ : 0;

    if(fi_mr_reg(domain_, read_buff_, len, FI_READ | FI_REMOTE_READ, 0, MR_KEY + 1, 0, &read_mr_, NULL)) {
      crash("RegistMR-R failed");
    }

    read_desc_ = fi_mr_desc(read_mr_);
    local_read_key_ = fi_mr_key(read_mr_);
    local_read_addr_ = vitr_addr ? (uint64_t)read_buff_ : 0;
  }

  void PullCQ() {
    cq_runner_ = std::make_unique<std::thread>([this] {
      int ret;
      fi_cq_err_entry comp;
      do {
        ret = fi_cq_read(cq_, &comp, 1);
        if (ret < 0 && ret != -FI_EAGAIN) {
          std::cout << "CQ PULLING ERROR: " << ret << "\t" << fi_strerror(-ret) << std::endl;
          return;
        }
        if(comp.flags & FI_READ) {
          std::cout << "CQ:FI_READ" << std::endl;
        } else if(comp.flags & FI_WRITE) {
          std::cout << "CQ:FI_WRITE" << std::endl;
        }
      } while(ret != 1);
    });
  }

  void Write(const std::string& msg) {
    std::memcpy(write_buff_, msg.data(), msg.size());
    int err = 0;
    do {
      err = fi_write(ep_, write_buff_, msg.size(), write_desc_, peers_[1], remote_write_addr_, remote_write_key_, NULL);
      if(err == -EAGAIN) {
        continue;
      }
      if(err) {
        std::cout << err << ": writing error:" << fi_strerror(-err) << std::endl;
      } else {
        std::cout << "WriteOK" << std::endl;
      }
    } while(err == -EAGAIN);
  }

  std::string_view DeviceName() const {
    return fi_->fabric_attr->name;
  }

  std::unique_ptr<std::thread> cq_runner_;

  fi_info *fi_ = nullptr;
  fid_fabric *fabric_ = nullptr;
  fid_domain *domain_ = nullptr;
  fid_av *av_ = nullptr;
  fid_ep *ep_ = nullptr;
  std::string ep_addr_;
  fid_cq *cq_ = nullptr;
  boost::asio::io_context io_context_;
  std::unique_ptr<std::thread> thread_;
  std::vector<fi_addr_t> peers_;
  std::unique_ptr<std::thread> poll_thread_;

  fid_mr* write_mr_ = nullptr;
  char* write_buff_ = nullptr;
  size_t write_buff_len_ = 0;

  int64_t local_write_key_ = 0;
  int64_t local_write_addr_ = 0;

  int64_t remote_write_key_ = 0;
  uint64_t remote_write_addr_ = 0;

  void* write_desc_ = nullptr;

  fid_mr* read_mr_ = nullptr;
  char* read_buff_ = nullptr;
  size_t read_buff_len_ = 0;

  int64_t local_read_key_ = 0;
  int64_t local_read_addr_ = 0;
  int64_t remote_read_key_ = 0;
  uint64_t remote_read_addr_ = 0;

  void* read_desc_ = nullptr;
  // Don't clear data
  std::vector<void*> writing_;
};

}
}

int main(int argc, char* argv[]) {
  ray::rdma::Fabric fi;
  fi.InitInfo(argv[1], FI_EP_RDM, FI_RMA);
  fi.InitFabric();
  fi.InitDomain();
  fi.InitEndpoint();
  fi.InitAV();
  fi.InitCQ(1028, FI_CQ_FORMAT_MSG);
  fi.Finish();
  fi.RegisterMR(64);

  if(argc == 2) {
    std::cout <<  "usage: fi prov port cli/ser" << std::endl;
    std::exit(1);
  }

  auto port = std::atoi(argv[2]);

  if(argc == 3) {
    fi.StartAsServer(port);
  } else {
    fi.ConnectToServer(port);
  }
  fi.PullCQ();

  while(std::cin) {
    std::string msg;
    std::getline(std::cin, msg);
    if(msg == "") {
      std::cout << "W: " <<fi.GetWriteBuffer() << std::endl;
      std::cout << "R: " <<fi.GetReadBuffer() << std::endl;
    } else {
      fi.Write(msg);
    }
  }

  std::this_thread::sleep_for(std::chrono::seconds(10000));

  return 0;
}
