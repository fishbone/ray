#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <boost/algorithm/hex.hpp>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_errno.h>
using namespace std;
#define CHK_ERR(name, cond, err)							\
	do {										\
		if (cond) {								\
			fprintf(stderr,"%s: %s\n", name, strerror(-(err)));		\
			exit(1);							\
		}									\
	} while (0)

static struct fi_info		*fi;
static struct fid_fabric	*fabric;
static struct fid_domain	*domain;
static struct fid_av		*av;
static struct fid_ep		*ep;
static struct fid_cq		*cq;
static fi_addr_t		peer_addr;
static struct fi_context	sctxt;
static struct fi_context	rctxt;
static char			sbuf[64];
static char			rbuf[64];
std::string nickname;

static void get_peer_addr(void *peer_name)
{
	int err;
	char buf[64];
	size_t len = 64;

	buf[0] = '\0';
	fi_av_straddr(av, peer_name, buf, &len);
	printf("Translating peer address: %s\n", buf);
    fi_addr_t pppt;
	err = fi_av_insert(av, peer_name, 1, &peer_addr, 0, NULL);
	CHK_ERR("fi_av_insert", (err!=1), err);
}

static void init_fabric()
{
	struct fi_info		*hints;
	struct fi_cq_attr	cq_attr;
	struct fi_av_attr	av_attr;
	int 			err;
	int			version;
	char			name[64], buf[64];
	size_t			len = 64;

	hints = fi_allocinfo();
	CHK_ERR("fi_allocinfo", (!hints), -ENOMEM);

	memset(&cq_attr, 0, sizeof(cq_attr));
	memset(&av_attr, 0, sizeof(av_attr));
	memset(name, 0, len);
	memset(buf, 0, len);

	hints->ep_attr->type = FI_EP_RDM;

    hints->caps = FI_MSG | FI_RMA;

	hints->fabric_attr->prov_name = strdup("efa");

    hints->domain_attr->mr_mode = FI_MR_LOCAL
      | FI_MR_VIRT_ADDR
      | FI_MR_ALLOCATED
      | FI_MR_PROV_KEY;


	version = FI_VERSION(1, 18);

    // hints->addr_format = FI_SOCKADDR_IN;

	err = fi_getinfo(version, NULL, "8080", FI_SOURCE, hints, &fi);

	CHK_ERR("fi_getinfo", (err<0), err);

	fi_freeinfo(hints);

	printf("Using OFI device: %s\n", fi->fabric_attr->name);

    auto tmp = fi;

    while(tmp) {
      cout << "====" << endl;
      cout << fi_tostr(tmp, FI_TYPE_INFO) << endl;
      tmp = tmp->next;
    }

	err = fi_fabric(fi->fabric_attr, &fabric, NULL);

	CHK_ERR("fi_fabric", (err<0), err);

	err = fi_domain(fabric, fi, &domain, NULL);

	CHK_ERR("fi_domain", (err<0), err);

	av_attr.type = FI_AV_MAP;

	err = fi_av_open(domain, &av_attr, &av, NULL);
	CHK_ERR("fi_av_open", (err<0), err);

	cq_attr.format = FI_CQ_FORMAT_TAGGED;
	cq_attr.size = 100;

	err = fi_cq_open(domain, &cq_attr, &cq, NULL);
	CHK_ERR("fi_cq_open", (err<0), err);

	err = fi_endpoint(domain, fi, &ep, NULL);
	CHK_ERR("fi_endpoint", (err<0), err);

	err = fi_ep_bind(ep, (fid_t)cq, FI_SEND|FI_RECV);
	CHK_ERR("fi_ep_bind cq", (err<0), err);

	err = fi_ep_bind(ep, (fid_t)av, 0);
	CHK_ERR("fi_ep_bind av", (err<0), err);

	err = fi_enable(ep);
	CHK_ERR("fi_enable", (err<0), err);

	err = fi_getname((fid_t)ep, name, &len);
	CHK_ERR("fi_getname", (err<0), err);

    cout << "ADDR:" << len << ": ";

	for (int i = 0; i < len; ++i){
      printf("%d ", name[i]);
	}

    cout << endl;

	buf[0] = '\0';

    len = 64;

	fi_av_straddr(av, name, buf, &len);

	printf("My address is %s\n", buf);

}

int static finalize_fabric(void) {
	fi_close((fid_t)ep);
	fi_close((fid_t)cq);
	fi_close((fid_t)av);
	fi_close((fid_t)domain);
	fi_close((fid_t)fabric);
	fi_freeinfo(fi);
}

static void wait_cq(void) {
	struct fi_cq_err_entry entry;
	int ret, completed = 0;
	fi_addr_t from;

	while (!completed) {
		ret = fi_cq_readfrom(cq, &entry, 1, &from);
		if (ret == -FI_EAGAIN)
			continue;

		if (ret == -FI_EAVAIL) {
			ret = fi_cq_readerr(cq, &entry, 1);
			CHK_ERR("fi_cq_readerr", (ret!=1), ret);

			printf("Completion with error: %d\n", entry.err);
			if (entry.err == FI_EADDRNOTAVAIL)
				get_peer_addr(entry.err_data);
		}

		CHK_ERR("fi_cq_read", (ret<0), ret);
		completed += ret;
	}
}

static void send_one(int size)
{
	int err;

    // CHK_ERR("send_one", (peer_addr==0ULL), -EDESTADDRREQ);
    cout << "Send to peer " << peer_addr << endl;

	char destAddr[64];
	char buf[64];

	size_t len = 64;

	destAddr[0] = '\0';

	err = fi_av_lookup(av, peer_addr, destAddr, &len);
    len = 64;
	fi_av_straddr(av, destAddr, buf, &len);
	printf("Send to peer address %s\n", buf);

    while(fi_send(ep, sbuf, size, NULL, peer_addr, &sctxt)) {
      perror("fi_send ");
    }

	wait_cq();
}

static void recv_one(int size)
{
	int err;

	err = fi_recv(ep, rbuf, size, NULL, FI_ADDR_UNSPEC, &rctxt);
	CHK_ERR("fi_recv", (err<0), err);

	wait_cq();
}


inline unsigned char hex_to_uchar(const char c, bool &err) {
  unsigned char num = 0;
  if (c >= '0' && c <= '9') {
    num = c - '0';
  } else if (c >= 'a' && c <= 'f') {
    num = c - 'a' + 0xa;
  } else if (c >= 'A' && c <= 'F') {
    num = c - 'A' + 0xA;
  } else {
    err = true;
  }
  return num;
}

int main(int argc, char *argv[]) {
	/* you need to modify the addr_dict for your own case*/

	char *server = NULL;
	int size = 64;

	if (argc < 2) {
		std::cout << "Usage, require endpoint nick name, e.g. ./fi_hello ep0 \n";
		return 1;
	}

	nickname = std::string(argv[1]);

    int is_client = argc == 3;



	std::string msg = "Hi server, this is "+nickname;
	strcpy(sbuf, msg.c_str());

	std::cout << "Input running mode: 0 stand for server, 1 for client\n";

	init_fabric();

    std::cout << "peer address" << endl;
    std::string dest;

    for(int i = 0; i < 32; ++i) {
      int c;
      cin >> c;
      dest.push_back(c);
    }

	// insert dst addr
	int numSuccess = 0;
    std::string bin_dest;

    // for (size_t i = 0; i < dest.size(); i++) {
    //   char first = dest[2 * i];
    //   char second = dest[2 * i + 1];
    //   bool err = false;
    //   bin_dest.push_back((hex_to_uchar(first, err) << 4) + hex_to_uchar(second, err));
    // }
    // cout << dest.size() << endl;
    // cout << "bin_addr_len: " << bin_dest.size() << endl;

	numSuccess = fi_av_insert(av, dest.data(), 1, &peer_addr, 0, NULL);

    cout << "check addr: " << numSuccess << endl;

	CHK_ERR("fi_av_insert", (numSuccess!=1), numSuccess);

	// ------ sentinel check inserted address
	char newbuf[64], newAddr[64]; size_t len = 64; int err = 0;
	memset(newbuf, 0, 64);
	memset(newAddr, 0, 64);
	err = fi_av_lookup(av, peer_addr, newAddr, &len);
	printf("look up error code %d\n retrieved addr buffer: \n", err);

	for (int i = 0; i < 64; i ++){
		printf("%d, ", newAddr[i]);
	}

	printf("\n");
	//
	// get the readable address for debugging;

    len = 64;
	fi_av_straddr(av, newAddr, newbuf, &len);
	printf("Send to peer address %s\n", newbuf);

	// ------ address check done

	if (is_client) {
		printf("Sending '%s' to server\n", sbuf);
		send_one(size);
		recv_one(size);
		printf("Received '%s' from server\n", rbuf);
	} else {
		while (1) {
			printf("Waiting for client\n");
			recv_one(size);
			printf("Received '%s' from client\n", rbuf);
			printf("Sending '%s' to client\n", sbuf);
			send_one(size);
			printf("Done, press RETURN to continue, 'q' to exit\n");
			if (getchar() == 'q')
				break;
		}
	}

	finalize_fabric();

	return 0;
}
