#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "platform/config/resdb_config.h"
#include "platform/networkstrate/replica_communicator.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"
#include "platform/consensus/ordering/pbft/paxos_manager.h"


namespace resdb {

// Forward declare to avoid circular include
class ConsensusManagerPBFT;

class TwoPhaseCommit {
 public:
  TwoPhaseCommit(const ResDBConfig& config,
                 ReplicaCommunicator* replica_communicator);
  ~TwoPhaseCommit();

  // ── Called from ConsensusManagerPBFT after construction ──────────────
  // Gives 2PC a back-pointer so ProcessPrepare can trigger intra-shard
  // PBFT before voting YES.
  void SetConsensusManager(ConsensusManagerPBFT* manager);

  PaxosManager* GetPaxosManager() { return paxos_manager_.get(); }

  // Coordinator: run full 2PC for a committed transaction
  int RunTwoPhaseCommit(const Request& committed_request);

  // Participant: handle PREPARE from coordinator
  int ProcessPrepare(std::unique_ptr<Request> request);

  // Coordinator: handle VOTE from participant
  int ProcessVote(std::unique_ptr<Request> request);

  // Participant: handle COMMIT from coordinator
  int ProcessCommit(std::unique_ptr<Request> request);

  bool IsCoordinator() const;

 private:
  void BroadcastPrepare(const Request& request);
  void BroadcastCommit(uint64_t seq);
  std::unique_ptr<PaxosManager> paxos_manager_;

  // Determine which node IDs are shard leaders
  // Leaders are nodes 1, 5, 9, 13 (first node of each group of 4)
  bool IsShardLeader(int node_id) const;
  std::set<int> GetShardLeaders() const;

  ResDBConfig config_;
  ReplicaCommunicator* replica_communicator_;
  ConsensusManagerPBFT* consensus_manager_ = nullptr;  // back-pointer
  Stats* global_stats_;
  int total_replicas_;  // number of shard leaders = 4

  struct TxnState {
    int vote_count = 0;
    std::condition_variable cv;
  };

  std::mutex mutex_;
  std::map<uint64_t, std::shared_ptr<TxnState>> txn_states_;
};

}  // namespace resdb