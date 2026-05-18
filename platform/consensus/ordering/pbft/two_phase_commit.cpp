#include "platform/consensus/ordering/pbft/two_phase_commit.h"
#include <glog/logging.h>
#include <chrono>

namespace resdb {

TwoPhaseCommit::TwoPhaseCommit(const ResDBConfig& config,
                               ReplicaCommunicator* replica_communicator)
    : config_(config), replica_communicator_(replica_communicator) {
  global_stats_ = Stats::GetGlobalStats();
  LOG(ERROR) << "[2PC] Init node:" << config_.GetSelfInfo().id()
             << " port:" << config_.GetSelfInfo().port()
             << " is_shard_leader:" << IsShardLeader()
             << " is_coordinator:" << IsCoordinator();
}

TwoPhaseCommit::~TwoPhaseCommit() {}

bool TwoPhaseCommit::IsShardLeader() const {
  int port = config_.GetSelfInfo().port();
  // Leaders are the first node of each shard: ports 10001, 10005, 10009, 10013
  for (int leader_port : shard_leader_ports_) {
    if (port == leader_port) return true;
  }
  return false;
}

bool TwoPhaseCommit::IsCoordinator() const {
  // The coordinating shard leader is determined by which leader
  // received the transaction. For simplicity, any shard leader
  // can be coordinator — we check IsShardLeader() at call site.
  return IsShardLeader();
}

// ============================================================
// COORDINATOR: Run full 2PC across all shard leaders
// ============================================================
int TwoPhaseCommit::RunTwoPhaseCommit(const Request& committed_request) {
  uint64_t seq = committed_request.seq();
  int my_port = config_.GetSelfInfo().port();
  LOG(ERROR) << "[2PC] Shard leader (port:" << my_port
             << ") starting 2PC for seq:" << seq;

  auto state = std::make_shared<TxnState>();
  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_[seq] = state;
  }

  // ---- Phase 1: Send PREPARE to all other shard leaders ----
  auto t0 = std::chrono::steady_clock::now();
  SendToShardLeaders(committed_request);
  LOG(ERROR) << "[2PC] Phase 1: PREPARE sent to all shard leaders, seq:" << seq;

  // Wait for votes from all OTHER shard leaders (num_shards_ - 1)
  // Plus our own implicit YES vote = num_shards_ total
  int needed_votes = num_shards_ - 1; // other 3 shard leaders vote
  {
    std::unique_lock<std::mutex> lk(mutex_);
    bool ok = state->cv.wait_for(lk, std::chrono::seconds(10),
      [&] { return state->vote_count >= needed_votes; });
    if (!ok) {
      LOG(ERROR) << "[2PC] TIMEOUT waiting for votes seq:" << seq;
      return -1;
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  long prepare_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      t1 - t0).count();
  LOG(ERROR) << "[2PC] Phase 1 DONE. All " << needed_votes
             << " shard leader votes received seq:" << seq
             << " in " << prepare_ms << "ms";

  // ---- Phase 2: Send COMMIT to all other shard leaders ----
  BroadcastCommitToShardLeaders(seq);
  auto t2 = std::chrono::steady_clock::now();
  long commit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      t2 - t1).count();
  LOG(ERROR) << "[2PC] Phase 2 DONE. COMMIT sent to all shard leaders seq:"
             << seq << " commit_ms:" << commit_ms
             << " total_ms:" << (prepare_ms + commit_ms);

  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_.erase(seq);
  }
  return 0;
}

void TwoPhaseCommit::SendToShardLeaders(const Request& request) {
  int my_port = config_.GetSelfInfo().port();
  for (int leader_port : shard_leader_ports_) {
    if (leader_port == my_port) continue; // skip self
    Request msg;
    msg.set_type(Request::TYPE_2PC_PREPARE);
    msg.set_seq(request.seq());
    msg.set_sender_id(config_.GetSelfInfo().id());
    msg.set_primary_id(config_.GetSelfInfo().id()); // coordinator id
    msg.set_current_view(config_.GetSelfInfo().port()); // store coordinator port for vote reply
    msg.set_data(request.data());

    // Build ReplicaInfo for the target shard leader
    ReplicaInfo target;
    target.set_ip("127.0.0.1");
    target.set_port(leader_port);

    LOG(ERROR) << "[2PC] Sending PREPARE to shard leader port:" << leader_port
               << " seq:" << request.seq();
    replica_communicator_->SendMessage(msg, target);
  }
}

void TwoPhaseCommit::BroadcastCommitToShardLeaders(uint64_t seq) {
  int my_port = config_.GetSelfInfo().port();
  for (int leader_port : shard_leader_ports_) {
    if (leader_port == my_port) continue;
    Request msg;
    msg.set_type(Request::TYPE_2PC_COMMIT);
    msg.set_seq(seq);
    msg.set_sender_id(config_.GetSelfInfo().id());

    ReplicaInfo target;
    target.set_ip("127.0.0.1");
    target.set_port(leader_port);

    LOG(ERROR) << "[2PC] Sending COMMIT to shard leader port:" << leader_port
               << " seq:" << seq;
    replica_communicator_->SendMessage(msg, target);
  }
}

// ============================================================
// PARTICIPANT: Handle PREPARE from coordinator shard leader
// ============================================================
int TwoPhaseCommit::ProcessPrepare(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int coordinator_id = request->sender_id();
  int coordinator_port = request->primary_id(); // we reuse primary_id for port
  LOG(ERROR) << "[2PC] Shard leader (port:" << config_.GetSelfInfo().port()
             << ") received PREPARE seq:" << seq
             << " from coordinator id:" << coordinator_id;

  // Always vote YES
  Request vote;
  vote.set_type(Request::TYPE_2PC_VOTE);
  vote.set_seq(seq);
  vote.set_sender_id(config_.GetSelfInfo().id());
  vote.set_ret(0); // YES

  // Send vote back to coordinator by port using ReplicaInfo
  // (coordinator_id reused as primary_id in the PREPARE message)
  ReplicaInfo coordinator;
  coordinator.set_ip("127.0.0.1");
  coordinator.set_port(request->current_view()); // we store coordinator port here
  replica_communicator_->SendMessage(vote, coordinator);
  LOG(ERROR) << "[2PC] Sending vote to coordinator port:" << request->current_view();
  LOG(ERROR) << "[2PC] Shard leader sent VOTE_YES seq:" << seq
             << " to coordinator id:" << coordinator_id;
  return 0;
}

// ============================================================
// COORDINATOR: Handle VOTE from participant shard leader
// ============================================================
int TwoPhaseCommit::ProcessVote(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int sender = request->sender_id();
  LOG(ERROR) << "[2PC] Coordinator received VOTE from shard leader id:" << sender
             << " seq:" << seq;

  std::shared_ptr<TxnState> state;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = txn_states_.find(seq);
    if (it == txn_states_.end()) {
      LOG(ERROR) << "[2PC] No state for seq:" << seq;
      return -1;
    }
    state = it->second;
    state->vote_count++;
    LOG(ERROR) << "[2PC] Vote count seq:" << seq
               << " = " << state->vote_count << "/" << (num_shards_ - 1);
  }
  state->cv.notify_all();
  return 0;
}

// ============================================================
// PARTICIPANT: Handle COMMIT from coordinator shard leader
// ============================================================
int TwoPhaseCommit::ProcessCommit(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  LOG(ERROR) << "[2PC] Shard leader (port:" << config_.GetSelfInfo().port()
             << ") received COMMIT seq:" << seq
             << " - executing transaction!";
  // Post-commit: this shard leader executes the transaction locally
  // (simplified option from assignment — all leaders execute)
  return 0;
}

}  // namespace resdb
