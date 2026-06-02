#pragma once

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include "platform/config/resdb_config.h"
#include "platform/networkstrate/replica_communicator.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"
#include "platform/consensus/ordering/pbft/paxos_manager.h"

namespace resdb {

class ConsensusManagerPBFT;

class TwoPhaseCommit {
 public:
  TwoPhaseCommit(const ResDBConfig& config,
                 ReplicaCommunicator* replica_communicator);
  ~TwoPhaseCommit();

  void SetConsensusManager(ConsensusManagerPBFT* manager);
  PaxosManager* GetPaxosManager() { return paxos_manager_.get(); }

  int RunTwoPhaseCommit(const Request& committed_request);
  int ProcessPrepare(std::unique_ptr<Request> request);
  int ProcessVote(std::unique_ptr<Request> request);
  int ProcessCommit(std::unique_ptr<Request> request);
  bool IsCoordinator() const;
  bool IsShardLeader() const;
  bool IsShardLeader(int node_id) const;

 private:
  void BroadcastPrepare(const Request& request);
  void BroadcastCommit(uint64_t seq);
  std::set<int> GetShardLeaders() const;

  ResDBConfig config_;
  ReplicaCommunicator* replica_communicator_;
  ConsensusManagerPBFT* consensus_manager_ = nullptr;
  std::unique_ptr<PaxosManager> paxos_manager_;
  Stats* global_stats_;
  int total_replicas_;

  struct TxnState {
    int vote_count = 0;
    std::condition_variable cv;
  };

  std::mutex mutex_;
  std::map<uint64_t, std::shared_ptr<TxnState>> txn_states_;
};

}  // namespace resdb
