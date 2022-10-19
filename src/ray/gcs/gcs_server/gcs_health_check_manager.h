// Copyright 2022 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

namespace ray {
namespace gcs {

/// GcsHealthCheckManager is used to track the healthiness of the nodes in the ray
/// cluster. The health check is done in pull based way, which means this module will send
/// health check to the raylets to see whether the raylet is healthy or not. If the raylet
/// is not healthy for certain times, the module will think the raylet dead.
class GcsHealthCheckManager {
 public:
  /// Constructor of GcsHealthCheckManager.
  ///
  /// \param io_service The thread where all operations in this class should run.
  /// \param on_node_death_callback The callback function when some node is marked as
  /// failure. \param initial_delay_ms The delay for the first health check. \param
  /// period_ms The interval between two health checks for the same node. \param
  /// failure_threshold The threshold before a node will be marked as dead due to health
  /// check failure.
  GcsHealthCheckManager(
      instrumented_io_context &io_service,
      std::function<void(const NodeID &)> on_node_death_callback,
      int64_t initial_delay_ms = RayConfig::instance().health_check_initial_delay_ms(),
      int64_t timeout_ms = RayConfig::instance().health_check_timeout_ms(),
      int64_t period_ms = RayConfig::instance().health_check_period_ms(),
      int64_t failure_threshold = RayConfig::instance().health_check_failure_threshold());

  ~GcsHealthCheckManager();

  /// Start to track the healthiness of a node.
  ///
  /// \param node_id The id of the node.
  /// \param channel The gRPC channel to the node.
  void AddNode(const NodeID &node_id, std::shared_ptr<grpc::Channel> channel);

  /// Stop tracking the healthiness of a node.
  ///
  /// \param node_id The id of the node to stop tracking.
  void RemoveNode(const NodeID &node_id);

 private:
  void FailNode(const NodeID &node_id);

  /// The context for the health check. It's to support unary call.
  /// It can be updated to support streaming call for efficiency.
  class HealthCheckContext {
   public:
    HealthCheckContext(GcsHealthCheckManager *_manager,
                       std::shared_ptr<grpc::Channel> channel,
                       NodeID node_id)
        : manager(_manager),
          node_id(node_id),
          timer(manager->io_service_),
          health_check_remaining(manager->failure_threshold_) {
      stub = grpc::health::v1::Health::NewStub(channel);
      timer.expires_from_now(boost::posix_time::milliseconds(manager->initial_delay_ms_));
      timer.async_wait([this](auto ec) {
        if (ec != boost::asio::error::operation_aborted) {
          StartHealthCheck();
        }
      });
    }

    ~HealthCheckContext() {
      timer.cancel();
      if (context != nullptr) {
        context->TryCancel();
      }
    }

   private:
    void StartHealthCheck();

    GcsHealthCheckManager *manager;

    NodeID node_id;

    /// gRPC related fields
    std::unique_ptr<::grpc::health::v1::Health::Stub> stub;
    std::unique_ptr<grpc::ClientContext> context;
    ::grpc::health::v1::HealthCheckRequest request;
    ::grpc::health::v1::HealthCheckResponse response;

    /// The timer is used to do async wait before the next try.
    boost::asio::deadline_timer timer;

    /// The remaining check left. If it reaches 0, the node will be marked as dead.
    int64_t health_check_remaining;
  };

  /// The main service. All method needs to run on this thread.
  instrumented_io_context &io_service_;

  /// Callback when the node failed.
  std::function<void(const NodeID &)> on_node_death_callback_;

  /// The context of the health check for each nodes.
  absl::flat_hash_map<NodeID, std::unique_ptr<HealthCheckContext>>
      inflight_health_checks_;

  /// The delay for the first health check request.
  const int64_t initial_delay_ms_;
  /// Timeout for each health check request.
  const int64_t timeout_ms_;
  /// Intervals between two health check.
  const int64_t period_ms_;
  /// The number of failures before the node is considered as dead.
  const int64_t failure_threshold_;
};

}  // namespace gcs
}  // namespace ray
