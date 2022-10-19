#include "ray/gcs/gcs_server/gcs_health_check_manager.h"

namespace ray {
namespace gcs {

GcsHealthCheckManager::GcsHealthCheckManager(
    instrumented_io_context &io_service,
    std::function<void(const NodeID &)> on_node_death_callback,
    int64_t initial_delay_ms,
    int64_t timeout_ms,
    int64_t period_ms,
    int64_t failure_threshold)
    : io_service_(io_service),
      on_node_death_callback_(on_node_death_callback),
      initial_delay_ms_(initial_delay_ms),
      timeout_ms_(timeout_ms),
      period_ms_(period_ms),
      failure_threshold_(failure_threshold) {}

GcsHealthCheckManager::~GcsHealthCheckManager() { inflight_health_checks_.clear(); }

void GcsHealthCheckManager::RemoveNode(const NodeID &node_id) {
  io_service_.dispatch(
      [this, node_id]() {
        auto iter = inflight_health_checks_.find(node_id);
        if (iter == inflight_health_checks_.end()) {
          return;
        }
        inflight_health_checks_.erase(iter);
      },
      "GcsHealthCheckManager::RemoveNode");
}

void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  on_node_death_callback_(node_id);
  inflight_health_checks_.erase(node_id);
}

void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;
  context = std::make_unique<grpc::ClientContext>();
  auto deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(manager->timeout_ms_);
  context->set_deadline(deadline);
  stub->async()->Check(context.get(), &request, &response, [this](::grpc::Status status) {
    manager->io_service_.post(
        [this, status]() {
          if (status.error_code() == ::grpc::StatusCode::CANCELLED) {
            manager->io_service_.post(
                [this]() { manager->inflight_health_checks_.erase(node_id); }, "");
          }

          if (status.ok() && response.status() == HealthCheckResponse::SERVING) {
            // Health check passed
            health_check_remaining = manager->failure_threshold_;
          } else {
            --health_check_remaining;
          }

          if (health_check_remaining == 0) {
            manager->io_service_.post([this]() { manager->FailNode(node_id); }, "");
          } else {
            // Do another health check.
            timer.expires_from_now(boost::posix_time::milliseconds(manager->period_ms_));
            timer.async_wait([this](auto ec) {
              if (ec != boost::asio::error::operation_aborted) {
                StartHealthCheck();
              }
            });
          }
        },
        "HealthCheck");
  });
}

void GcsHealthCheckManager::AddNode(const NodeID &node_id,
                                    std::shared_ptr<grpc::Channel> channel) {
  io_service_.dispatch(
      [this, channel, node_id]() {
        RAY_CHECK(inflight_health_checks_.count(node_id) == 0);
        auto context = std::make_unique<HealthCheckContext>(this, channel, node_id);
        inflight_health_checks_.emplace(std::make_pair(node_id, std::move(context)));
      },
      "GcsHealthCheckManager::AddNode");
}

}  // namespace gcs
}  // namespace ray
