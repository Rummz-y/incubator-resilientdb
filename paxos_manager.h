#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include "platform/config/resdb_config.h"
#include "platform/networkstrate/replica_communicator.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {

// PaxosManager implements single-decree Paxos within one shard.
//
// On the shard LEADER (proposer):
//   Call RunPaxos(request) — blocks until majority of shard
//   replicas accept, then returns.
//
// On shard FOLLOWERS (acceptors):
//   Call ProcessPrepare / ProcessAccept for inbound messages.
//
// Quorum = majority of shard size (3 of 4 nodes per shard).

class PaxosManager {
 public:
  PaxosManager(const ResDBConfig& config,
               ReplicaCommunicator* replica_communicator,
               int shard_id,            // 1,2,3,4
               std::vector<int> shard_members); // all node IDs in this shard

  // Leader: run full Paxos round for this request.
  // Blocks until quorum reached. Returns 0 on success.
  int RunPaxos(const Request& request);

  // Follower: handle inbound PAXOS_PREPARE from leader
  int ProcessPrepare(std::unique_ptr<Request> request);

  // Follower: handle inbound PAXOS_ACCEPT from leader
  int ProcessAccept(std::unique_ptr<Request> request);

  // Leader: handle inbound PAXOS_PROMISE from follower
  int ProcessPromise(std::unique_ptr<Request> request);

  // Leader: handle inbound PAXOS_ACCEPTED from follower
  int ProcessAccepted(std::unique_ptr<Request> request);

 private:
  // Phase 1: broadcast PAXOS_PREPARE(n) to shard members
  void BroadcastPrepare(uint64_t proposal_num, uint64_t seq);

  // Phase 2: broadcast PAXOS_ACCEPT(n, v) to shard members
  void BroadcastAccept(uint64_t proposal_num, uint64_t seq,
                       const std::string& value);

  int QuorumSize() const;  // majority of shard

  // Per-round state tracked by the leader
  struct RoundState {
    int      promise_count  = 0;
    int      accepted_count = 0;
    std::condition_variable cv_promise;
    std::condition_variable cv_accepted;
  };

  // Acceptor state (used by followers)
  struct AcceptorState {
    uint64_t    promised_n  = 0;   // highest prepare seen
    uint64_t    accepted_n  = 0;   // proposal number accepted
    std::string accepted_v;        // value accepted
  };

  const ResDBConfig&    config_;
  ReplicaCommunicator*  replica_communicator_;
  int                   shard_id_;
  std::vector<int>      shard_members_;
  int                   self_id_;

  // Leader side
  std::mutex                                    round_mu_;
  std::map<uint64_t, std::shared_ptr<RoundState>> rounds_;  // keyed by seq

  // Follower side
  std::mutex       acceptor_mu_;
  AcceptorState    acceptor_state_;

  Stats* global_stats_;
};

}  // namespace resdb
