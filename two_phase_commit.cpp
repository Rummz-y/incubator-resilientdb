#include "platform/consensus/ordering/pbft/two_phase_commit.h"
#include "platform/consensus/ordering/pbft/algorithm/consensus_manager_pbft.h"
#include <glog/logging.h>
#include <chrono>

namespace resdb {

TwoPhaseCommit::TwoPhaseCommit(const ResDBConfig& config,
                               ReplicaCommunicator* replica_communicator)
    : config_(config), replica_communicator_(replica_communicator) {
  global_stats_ = Stats::GetGlobalStats();

  // Count shard leaders only — one per shard of 4
  // nodes 1,5,9,13 are leaders; total = 4
  total_replicas_ = GetShardLeaders().size();

  LOG(ERROR) << "[2PC] Init node:" << config_.GetSelfInfo().id()
             << " port:" << config_.GetSelfInfo().port()
             << " is_shard_leader:" << IsShardLeader(config_.GetSelfInfo().id())
             << " is_coordinator:" << IsCoordinator()
             << " total_shard_leaders:" << total_replicas_;
}

TwoPhaseCommit::~TwoPhaseCommit() {}

// ── Helper: which nodes are shard leaders ────────────────────────────────────
// global.config has 16 nodes in groups of 4.
// Leaders are the first node of each group: 1, 5, 9, 13.

bool TwoPhaseCommit::IsShardLeader(int node_id) const {
  return (node_id == 1 || node_id == 5 ||
          node_id == 9 || node_id == 13);
}

std::set<int> TwoPhaseCommit::GetShardLeaders() const {
  return {1, 5, 9, 13};
}

// ── Back-pointer setter ───────────────────────────────────────────────────────

void TwoPhaseCommit::SetConsensusManager(ConsensusManagerPBFT* manager) {
  consensus_manager_ = manager;
}

bool TwoPhaseCommit::IsCoordinator() const {
  // Node 1 is the permanent coordinator (shard 1 leader, receives proxy traffic)
  return config_.GetSelfInfo().id() == 1;
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

  // ── Phase 1: send PREPARE to all shard leaders ───────────────────────
  auto t0 = std::chrono::steady_clock::now();
  BroadcastPrepare(committed_request);
  LOG(ERROR) << "[2PC] Phase 1: PREPARE sent to shard leaders for seq:" << seq;

  // Wait for all shard leaders to vote YES
  // Each leader runs intra-shard PBFT before voting, so this may take
  // longer than a simple ACK — use a generous timeout
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

  // ── Phase 2: send COMMIT to all shard leaders ────────────────────────
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

  for (int leader_id : leaders) {
    if (leader_id == self_id) {
      // Coordinator handles its own shard via local PBFT below in ProcessPrepare
      // but since coordinator IS the leader for shard 1, we trigger it directly
      continue;
    }
    Request msg;
    msg.set_type(Request::TYPE_2PC_PREPARE);
    msg.set_seq(request.seq());
    msg.set_sender_id(self_id);
    msg.set_current_view(request.current_view());
    msg.set_data(request.data());
    replica_communicator_->SendMessage(msg, leader_id);
  }

  // Coordinator also runs intra-shard PBFT for its own shard (shard 1)
  // and counts itself as one vote immediately after
  LOG(ERROR) << "[2PC] Coordinator running intra-shard PBFT for shard 1 seq:"
             << request.seq();

  if (consensus_manager_ != nullptr) {
    auto ctx = std::make_unique<Context>();
    auto req = std::make_unique<Request>(request);
    req->set_type(Request::TYPE_NEW_TXNS);
    int ret = consensus_manager_->TriggerIntraShardConsensus(
        std::move(ctx), std::move(req));
    LOG(ERROR) << "[2PC] Coordinator shard 1 PBFT result:" << ret
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

// ── Participant: receive PREPARE, run intra-shard PBFT, vote YES ──────────────

int TwoPhaseCommit::ProcessPrepare(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int coordinator_id = request->sender_id();

  LOG(ERROR) << "[2PC] Shard leader node:" << config_.GetSelfInfo().id()
             << " received PREPARE seq:" << seq
             << " from coordinator:" << coordinator_id
             << " — running intra-shard PBFT before voting";

  // ── KEY CHANGE: run PBFT with this shard's followers first ───────────
  // Only do this if we have the back-pointer to the consensus manager
  if (consensus_manager_ != nullptr) {
    auto ctx = std::make_unique<Context>();
    auto req = std::make_unique<Request>(*request);
    req->set_type(Request::TYPE_NEW_TXNS);

    int ret = consensus_manager_->TriggerIntraShardConsensus(
        std::move(ctx), std::move(req));

    if (ret != 0) {
      LOG(ERROR) << "[2PC] Intra-shard PBFT FAILED node:"
                 << config_.GetSelfInfo().id()
                 << " seq:" << seq << " ret:" << ret;
      // Assignment says no aborts — log and continue anyway
    } else {
      LOG(ERROR) << "[2PC] Intra-shard PBFT DONE node:"
                 << config_.GetSelfInfo().id()
                 << " seq:" << seq << " — sending VOTE_YES";
    }
  } else {
    LOG(ERROR) << "[2PC] WARNING: no consensus_manager_ set on node:"
               << config_.GetSelfInfo().id()
               << " — voting YES without intra-shard consensus";
  }

  // Send VOTE_YES back to coordinator
  Request vote;
  vote.set_type(Request::TYPE_2PC_VOTE);
  vote.set_seq(seq);
  vote.set_sender_id(config_.GetSelfInfo().id());
  vote.set_ret(0);  // YES
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
      LOG(ERROR) << "[2PC] No state for seq:" << seq << " (already committed?)";
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