#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <vector>

#include "platform/config/resdb_config.h"
#include "platform/networkstrate/replica_communicator.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {

class TwoPhaseCommit {
 public:
  TwoPhaseCommit(const ResDBConfig& config,
                 ReplicaCommunicator* replica_communicator);
  ~TwoPhaseCommit();

  // Coordinator: run full 2PC across shard leaders
  int RunTwoPhaseCommit(const Request& committed_request);

  // Participant: handle PREPARE from coordinator shard leader
  int ProcessPrepare(std::unique_ptr<Request> request);

  // Coordinator: handle VOTE from participant shard leader
  int ProcessVote(std::unique_ptr<Request> request);

  // Participant: handle COMMIT from coordinator shard leader
  int ProcessCommit(std::unique_ptr<Request> request);

  // Returns true if this node is a shard leader (ids 1, 5, 9, 13)
  bool IsShardLeader() const;

  // Returns true if this node is THE coordinator (shard leader that
  // received the transaction — we use node 1 as default coordinator)
  bool IsCoordinator() const;

 private:
  void SendToShardLeaders(const Request& request);
  void BroadcastCommitToShardLeaders(uint64_t seq);

  ResDBConfig config_;
  ReplicaCommunicator* replica_communicator_;
  Stats* global_stats_;

  // The 4 shard leader node IDs and ports
  // Leaders: node1(10001), node5(10005), node9(10009), node13(10013)
  std::vector<int> shard_leader_ports_ = {10001, 10005, 10009, 10013};
  int num_shards_ = 4;

  struct TxnState {
    int vote_count = 0;
    std::condition_variable cv;
  };

  std::mutex mutex_;
  std::map<uint64_t, std::shared_ptr<TxnState>> txn_states_;
};

}  // namespace resdb
