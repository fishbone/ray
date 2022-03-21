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

namespace ray {
namespace syncer {

/// NodeStatus keeps track of the modules in the local nodes.
/// It contains the local components for receiving and reporting.
/// It also keeps the raw messages receivers got.
class NodeStatus {
 public:
  /// Set the local components.
  ///
  /// \param cid The component id.
  /// \param reporter The reporter is defined to be the local module which wants to
  /// broadcast its internal status to the whole clsuter. When it's null, it means there
  /// is no reporter in this node for this component. This is the place there messages are
  /// generated.
  /// \param receiver The receiver is defined to be the module which eventually
  /// will have the view of of the cluster for this component. It's the place where
  /// received messages are consumed.
  ///
  /// \return true if set successfully.
  bool SetComponents(RayComponentId cid,
                     const ReporterInterface *reporter,
                     ReceiverInterface *receiver);

  /// Get the snapshot of a component for a newer version.
  ///
  /// \param cid The component id to take the snapshot.
  ///
  /// \return If a snapshot is taken, return the message, otherwise std::nullopt.
  std::optional<RaySyncMessage> GetSnapshot(RayComponentId cid);

  /// Consume a message. Receiver will consume this message if it doesn't have
  /// this message.
  ///
  /// \param message The message received.
  ///
  /// \return true if this node doesn't have message with newer version.
  bool ConsumeMessage(std::shared_ptr<RaySyncMessage> message);

 private:
  /// For local nodes
  Array<const ReporterInterface *> reporters_ = {nullptr};
  Array<ReceiverInterface *> receivers_ = {nullptr};

  /// This fields records the version of the snapshot that has been taken.
  Array<uint64_t> snapshots_taken_ = {0};
  /// Keep track of the latest messages received.
  /// Use shared pointer for easier liveness management since these messages might be
  /// sending via rpc.
  absl::flat_hash_map<std::string, Array<std::shared_ptr<RaySyncMessage>>> cluster_view_;
};

class NodeSyncConnection {
 public:
  NodeSyncConnection(RaySyncer &instance,
                     instrumented_io_context &io_context,
                     std::string node_id);

  /// Push a message to the sending queue to be sent later.
  ///
  /// \param message The message to be sent.
  void PushToSendingQueue(std::shared_ptr<RaySyncMessage> message);

  virtual ~NodeSyncConnection() {}

  /// Return the node id of this sync context.
  const std::string &GetNodeId() const { return node_id_; }

  /// Handle the udpates sent from this node.
  ///
  /// \param messages The message received.
  void ReceiveUpdate(RaySyncMessages messages) {
    for (auto &message : *messages.mutable_sync_messages()) {
      auto &node_versions = GetNodeComponentVersions(message.node_id());
      if (node_versions[message.component_id()] < message.version()) {
        node_versions[message.component_id()] = message.version();
      }
      instance_.BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(message)));
    }
  }

 protected:
  std::array<uint64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id);
  ray::PeriodicalRunner timer_;
  RaySyncer &instance_;
  instrumented_io_context &io_context_;
  std::string node_id_;

  struct _MessageHash {
    std::size_t operator()(const std::shared_ptr<RaySyncMessage> &m) const noexcept {
      std::size_t seed = 0;
      boost::hash_combine(seed, m->node_id());
      boost::hash_combine(seed, m->component_id());
      return seed;
    }
  };

  absl::flat_hash_set<std::shared_ptr<RaySyncMessage>, _MessageHash> sending_queue_;
  // Keep track of the versions of components in this node.
  absl::flat_hash_map<std::string, std::array<uint64_t, kComponentArraySize>>
      node_versions_;
};

class ServerSyncConnection : public NodeSyncConnection {
 public:
  ServerSyncConnection(RaySyncer &instance,
                       instrumented_io_context &io_context,
                       const std::string &node_id);

  void HandleLongPollingRequest(grpc::ServerUnaryReactor *reactor,
                                RaySyncMessages *response);

  void DoSend();

 protected:
  // These two fields are RPC related. When the server got long-polling requests,
  // these two fields will be set so that it can be used to send message.
  // After the message being sent, these two fields will be set to be empty again.
  // When the periodical timer wake up, it'll check whether these two fields are set
  // and it'll only send data when these are set.
  RaySyncMessages *response_ = nullptr;
  grpc::ServerUnaryReactor *unary_reactor_ = nullptr;
};

class ClientSyncConnection : public NodeSyncConnection {
 public:
  ClientSyncConnection(RaySyncer &instance,
                       instrumented_io_context &io_context,
                       const std::string &node_id,
                       std::shared_ptr<grpc::Channel> channel);

  void DoSend();

 protected:
  /// Start to send long-polling request to remote nodes.
  void StartLongPolling();

  /// Stub for this connection.
  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;
  ray::rpc::syncer::RaySyncMessages in_message_;
  StartSyncRequest start_sync_request_;
  StartSyncResponse start_sync_response_;
  bool connection_created_ = false;
  DummyRequest dummy_;
};

}  // namespace syncer
}  // namespace ray
