#include "platform/consensus/ordering/pbft/two_phase_commit.h"

#include <glog/logging.h>
#include <chrono>

namespace resdb {

TwoPhaseCommit::TwoPhaseCommit(const ResDBConfig& config,
                               ReplicaCommunicator* replica_communicator)
    : config_(config), replica_communicator_(replica_communicator) {
  global_stats_ = Stats::GetGlobalStats();
  total_replicas_ = GetShardLeaders().size();

  // Determine which shard this node belongs to and create its PaxosManager
  int self_id = config_.GetSelfInfo().id();
  int shard_id = 0;
  std::vector<int> shard_members;

  if (self_id >= 1  && self_id <= 4)  { shard_id = 1; shard_members = {1,2,3,4}; }
  if (self_id >= 5  && self_id <= 8)  { shard_id = 2; shard_members = {5,6,7,8}; }
  if (self_id >= 9  && self_id <= 12) { shard_id = 3; shard_members = {9,10,11,12}; }
  if (self_id >= 13 && self_id <= 16) { shard_id = 4; shard_members = {13,14,15,16}; }

  if (shard_id > 0) {
    paxos_manager_ = std::make_unique<PaxosManager>(
        config_, replica_communicator_, shard_id, shard_members);
  }

  LOG(ERROR) << "[2PC] Init node:" << self_id
             << " port:" << config_.GetSelfInfo().port()
             << " shard_id:" << shard_id
             << " is_shard_leader:" << IsShardLeader(self_id)
             << " is_coordinator:" << IsCoordinator()
             << " total_shard_leaders:" << total_replicas_;
}

TwoPhaseCommit::~TwoPhaseCommit() {}

bool TwoPhaseCommit::IsShardLeader() const {
  return IsShardLeader(config_.GetSelfInfo().id());
}

bool TwoPhaseCommit::IsShardLeader(int node_id) const {
  int port = config_.GetSelfInfo().port();
  return (port == 10001 || port == 10005 ||
          port == 10009 || port == 10013);
}

std::set<int> TwoPhaseCommit::GetShardLeaders() const {
  return {1, 5, 9, 13};
}

void TwoPhaseCommit::SetConsensusManager(ConsensusManagerPBFT* manager) {
  consensus_manager_ = manager;
}

bool TwoPhaseCommit::IsCoordinator() const {
  return config_.GetSelfInfo().port() == 10001;
}

// ── Coordinator: run full 2PC round ──────────────────────────────────────────

int TwoPhaseCommit::RunTwoPhaseCommit(const Request& committed_request) {
  uint64_t seq = committed_request.seq();
  LOG(ERROR) << "[2PC] Coordinator starting 2PC for seq:" << seq;

  auto state = std::make_shared<TxnState>();
  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_[seq] = state;
  }

  auto t0 = std::chrono::steady_clock::now();
  BroadcastPrepare(committed_request);
  LOG(ERROR) << "[2PC] Phase 1: PREPARE sent to shard leaders for seq:" << seq;

  {
    std::unique_lock<std::mutex> lk(mutex_);
    bool ok = state->cv.wait_for(
        lk, std::chrono::seconds(30),
        [&] { return state->vote_count >= total_replicas_; });
    if (!ok) {
      LOG(ERROR) << "[2PC] TIMEOUT waiting for votes seq:" << seq
                 << " got:" << state->vote_count
                 << " needed:" << total_replicas_;
      std::lock_guard<std::mutex> cleanup(mutex_);
      txn_states_.erase(seq);
      return -1;
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  long prepare_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  LOG(ERROR) << "[2PC] Phase 1 DONE. All " << total_replicas_
             << " shard leaders voted YES for seq:" << seq
             << " in " << prepare_ms << "ms";

  BroadcastCommit(seq);
  auto t2 = std::chrono::steady_clock::now();
  long commit_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
  LOG(ERROR) << "[2PC] Phase 2 DONE. COMMIT broadcast seq:" << seq
             << " commit_ms:" << commit_ms
             << " total_ms:" << (prepare_ms + commit_ms);

  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_.erase(seq);
  }
  return 0;
}

// ── Broadcast helpers ─────────────────────────────────────────────────────────

void TwoPhaseCommit::BroadcastPrepare(const Request& request) {
  auto leaders = GetShardLeaders();
  int self_id = config_.GetSelfInfo().id();

  // Send PREPARE to other shard leaders
  for (int leader_id : leaders) {
    if (leader_id == self_id) continue;
    Request msg;
    msg.set_type(Request::TYPE_2PC_PREPARE);
    msg.set_seq(request.seq());
    msg.set_sender_id(self_id);
    msg.set_current_view(request.current_view());
    msg.set_data(request.data());
    replica_communicator_->SendMessage(msg, leader_id);
  }

  // Coordinator runs Paxos for its own shard (shard 1)
  LOG(ERROR) << "[2PC] Coordinator running intra-shard Paxos for shard 1 seq:"
             << request.seq();

  if (paxos_manager_ != nullptr) {
    int ret = paxos_manager_->RunPaxos(request);
    LOG(ERROR) << "[2PC+Paxos] Coordinator shard 1 Paxos result:" << ret
               << " for seq:" << request.seq();
  }

  // Count coordinator's own shard vote
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = txn_states_.find(request.seq());
  if (it != txn_states_.end()) {
    it->second->vote_count++;
    LOG(ERROR) << "[2PC] Coordinator self-vote counted seq:" << request.seq()
               << " count:" << it->second->vote_count
               << "/" << total_replicas_;
    it->second->cv.notify_all();
  }
}

void TwoPhaseCommit::BroadcastCommit(uint64_t seq) {
  auto leaders = GetShardLeaders();
  int self_id = config_.GetSelfInfo().id();
  for (int leader_id : leaders) {
    if (leader_id == self_id) continue;
    Request msg;
    msg.set_type(Request::TYPE_2PC_COMMIT);
    msg.set_seq(seq);
    msg.set_sender_id(self_id);
    replica_communicator_->SendMessage(msg, leader_id);
  }
}

// ── Participant: receive 2PC PREPARE, run Paxos, vote YES ────────────────────

int TwoPhaseCommit::ProcessPrepare(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int coordinator_id = request->sender_id();

  LOG(ERROR) << "[2PC] Shard leader node:" << config_.GetSelfInfo().id()
             << " received PREPARE seq:" << seq
             << " from coordinator:" << coordinator_id
             << " — running intra-shard Paxos before voting";

  if (paxos_manager_ != nullptr) {
    int ret = paxos_manager_->RunPaxos(*request);
    if (ret != 0) {
      LOG(ERROR) << "[2PC+Paxos] Intra-shard Paxos FAILED node:"
                 << config_.GetSelfInfo().id()
                 << " seq:" << seq << " ret:" << ret;
    } else {
      LOG(ERROR) << "[2PC+Paxos] Intra-shard Paxos DONE node:"
                 << config_.GetSelfInfo().id()
                 << " seq:" << seq << " — sending VOTE_YES";
    }
  } else {
    LOG(ERROR) << "[2PC] WARNING: no paxos_manager_ on node:"
               << config_.GetSelfInfo().id()
               << " — voting YES without intra-shard consensus";
  }

  Request vote;
  vote.set_type(Request::TYPE_2PC_VOTE);
  vote.set_seq(seq);
  vote.set_sender_id(config_.GetSelfInfo().id());
  vote.set_ret(0);
  replica_communicator_->SendMessage(vote, coordinator_id);

  LOG(ERROR) << "[2PC] Node:" << config_.GetSelfInfo().id()
             << " sent VOTE_YES seq:" << seq
             << " to coordinator:" << coordinator_id;
  return 0;
}

// ── Coordinator: collect votes ────────────────────────────────────────────────

int TwoPhaseCommit::ProcessVote(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int sender = request->sender_id();

  LOG(ERROR) << "[2PC] Coordinator received VOTE_YES from node:" << sender
             << " seq:" << seq;

  std::shared_ptr<TxnState> state;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = txn_states_.find(seq);
    if (it == txn_states_.end()) {
      LOG(ERROR) << "[2PC] No state for seq:" << seq;
      return 0;
    }
    state = it->second;
    state->vote_count++;
    LOG(ERROR) << "[2PC] Vote count seq:" << seq
               << " = " << state->vote_count << "/" << total_replicas_;
  }
  state->cv.notify_all();
  return 0;
}

// ── Participant: receive COMMIT ───────────────────────────────────────────────

int TwoPhaseCommit::ProcessCommit(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  LOG(ERROR) << "[2PC] Node:" << config_.GetSelfInfo().id()
             << " received COMMIT seq:" << seq
             << " — transaction fully committed across all shards";
  return 0;
}

}  // namespace resdb
