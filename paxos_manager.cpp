#include "platform/consensus/ordering/pbft/paxos_manager.h"
#include <glog/logging.h>
#include <chrono>

namespace resdb {

PaxosManager::PaxosManager(const ResDBConfig& config,
                            ReplicaCommunicator* replica_communicator,
                            int shard_id,
                            std::vector<int> shard_members)
    : config_(config),
      replica_communicator_(replica_communicator),
      shard_id_(shard_id),
      shard_members_(shard_members),
      self_id_(config.GetSelfInfo().id()) {
  global_stats_ = Stats::GetGlobalStats();
  LOG(ERROR) << "[Paxos] Init shard:" << shard_id_
             << " node:" << self_id_
             << " members:" << shard_members_.size()
             << " quorum:" << QuorumSize();
}

int PaxosManager::QuorumSize() const {
  // Simple majority
  return (shard_members_.size() / 2) + 1;
}

// ── Leader: run full Paxos round ─────────────────────────────────────────────

int PaxosManager::RunPaxos(const Request& request) {
  uint64_t seq = request.seq();
  // Use seq as proposal number — monotonically increasing, unique per round
  uint64_t proposal_num = seq;

  LOG(ERROR) << "[Paxos] Leader node:" << self_id_
             << " shard:" << shard_id_
             << " starting Paxos seq:" << seq
             << " proposal_num:" << proposal_num;

  // Create round state
  auto state = std::make_shared<RoundState>();
  {
    std::lock_guard<std::mutex> lk(round_mu_);
    rounds_[seq] = state;
  }

  // ── Phase 1: Prepare ─────────────────────────────────────────────────
  auto t0 = std::chrono::steady_clock::now();
  BroadcastPrepare(proposal_num, seq);

  // Leader counts its own promise immediately
  {
    std::lock_guard<std::mutex> lk(round_mu_);
    state->promise_count++;
    LOG(ERROR) << "[Paxos] Leader self-promise shard:" << shard_id_
               << " seq:" << seq
               << " count:" << state->promise_count
               << "/" << QuorumSize();
  }

  // Wait for quorum of promises
  {
    std::unique_lock<std::mutex> lk(round_mu_);
    bool ok = state->cv_promise.wait_for(
        lk, std::chrono::seconds(10),
        [&] { return state->promise_count >= QuorumSize(); });
    if (!ok) {
      LOG(ERROR) << "[Paxos] TIMEOUT waiting for promises shard:" << shard_id_
                 << " seq:" << seq
                 << " got:" << state->promise_count
                 << " need:" << QuorumSize();
      std::lock_guard<std::mutex> cleanup(round_mu_);
      rounds_.erase(seq);
      return -1;
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  long phase1_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  LOG(ERROR) << "[Paxos] Phase 1 DONE shard:" << shard_id_
             << " seq:" << seq
             << " quorum promises in " << phase1_ms << "ms";

  // ── Phase 2: Accept ──────────────────────────────────────────────────
  BroadcastAccept(proposal_num, seq, request.data());

  // Leader counts its own accept immediately
  {
    std::lock_guard<std::mutex> lk(round_mu_);
    state->accepted_count++;
    LOG(ERROR) << "[Paxos] Leader self-accept shard:" << shard_id_
               << " seq:" << seq
               << " count:" << state->accepted_count
               << "/" << QuorumSize();
  }

  // Wait for quorum of accepted
  {
    std::unique_lock<std::mutex> lk(round_mu_);
    bool ok = state->cv_accepted.wait_for(
        lk, std::chrono::seconds(10),
        [&] { return state->accepted_count >= QuorumSize(); });
    if (!ok) {
      LOG(ERROR) << "[Paxos] TIMEOUT waiting for accepted shard:" << shard_id_
                 << " seq:" << seq;
      std::lock_guard<std::mutex> cleanup(round_mu_);
      rounds_.erase(seq);
      return -1;
    }
  }

  auto t2 = std::chrono::steady_clock::now();
  long phase2_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
  LOG(ERROR) << "[Paxos] Phase 2 DONE shard:" << shard_id_
             << " seq:" << seq
             << " value chosen in " << phase2_ms << "ms"
             << " total_ms:" << (phase1_ms + phase2_ms);

  {
    std::lock_guard<std::mutex> lk(round_mu_);
    rounds_.erase(seq);
  }
  return 0;
}

// ── Broadcast helpers ─────────────────────────────────────────────────────────

void PaxosManager::BroadcastPrepare(uint64_t proposal_num, uint64_t seq) {
  for (int member_id : shard_members_) {
    if (member_id == self_id_) continue;
    Request msg;
    msg.set_type(Request::TYPE_PAXOS_PREPARE);
    msg.set_seq(seq);
    msg.set_sender_id(self_id_);
    msg.set_current_view(proposal_num);  // reuse current_view to carry n
    replica_communicator_->SendMessage(msg, member_id);
  }
  LOG(ERROR) << "[Paxos] PREPARE broadcast shard:" << shard_id_
             << " seq:" << seq
             << " proposal_num:" << proposal_num;
}

void PaxosManager::BroadcastAccept(uint64_t proposal_num, uint64_t seq,
                                    const std::string& value) {
  for (int member_id : shard_members_) {
    if (member_id == self_id_) continue;
    Request msg;
    msg.set_type(Request::TYPE_PAXOS_ACCEPT);
    msg.set_seq(seq);
    msg.set_sender_id(self_id_);
    msg.set_current_view(proposal_num);
    msg.set_data(value);
    replica_communicator_->SendMessage(msg, member_id);
  }
  LOG(ERROR) << "[Paxos] ACCEPT broadcast shard:" << shard_id_
             << " seq:" << seq
             << " proposal_num:" << proposal_num;
}

// ── Follower: handle PREPARE → send PROMISE ───────────────────────────────────

int PaxosManager::ProcessPrepare(std::unique_ptr<Request> request) {
  uint64_t proposal_num = request->current_view();
  uint64_t seq          = request->seq();
  int      leader_id    = request->sender_id();

  LOG(ERROR) << "[Paxos] Follower node:" << self_id_
             << " shard:" << shard_id_
             << " received PREPARE seq:" << seq
             << " proposal_num:" << proposal_num
             << " from leader:" << leader_id;

  std::lock_guard<std::mutex> lk(acceptor_mu_);

  // Paxos safety: only promise if proposal_num > promised_n
  if (proposal_num <= acceptor_state_.promised_n) {
    LOG(ERROR) << "[Paxos] Follower node:" << self_id_
               << " REJECTING prepare n:" << proposal_num
               << " already promised:" << acceptor_state_.promised_n;
    return 0;  // assignment: no aborts, so just return
  }

  acceptor_state_.promised_n = proposal_num;

  // Send PROMISE back to leader
  Request promise;
  promise.set_type(Request::TYPE_PAXOS_PROMISE);
  promise.set_seq(seq);
  promise.set_sender_id(self_id_);
  promise.set_current_view(proposal_num);
  // Include previously accepted value if any (for Paxos correctness)
  if (acceptor_state_.accepted_n > 0) {
    promise.set_data(acceptor_state_.accepted_v);
  }
  replica_communicator_->SendMessage(promise, leader_id);

  LOG(ERROR) << "[Paxos] Follower node:" << self_id_
             << " sent PROMISE seq:" << seq
             << " to leader:" << leader_id;
  return 0;
}

// ── Follower: handle ACCEPT → send ACCEPTED ───────────────────────────────────

int PaxosManager::ProcessAccept(std::unique_ptr<Request> request) {
  uint64_t proposal_num = request->current_view();
  uint64_t seq          = request->seq();
  int      leader_id    = request->sender_id();

  LOG(ERROR) << "[Paxos] Follower node:" << self_id_
             << " shard:" << shard_id_
             << " received ACCEPT seq:" << seq
             << " proposal_num:" << proposal_num
             << " from leader:" << leader_id;

  std::lock_guard<std::mutex> lk(acceptor_mu_);

  // Only accept if proposal_num >= promised_n
  if (proposal_num < acceptor_state_.promised_n) {
    LOG(ERROR) << "[Paxos] Follower node:" << self_id_
               << " REJECTING accept n:" << proposal_num
               << " promised:" << acceptor_state_.promised_n;
    return 0;
  }

  // Accept the value
  acceptor_state_.accepted_n = proposal_num;
  acceptor_state_.accepted_v = request->data();

  LOG(ERROR) << "[Paxos] Follower node:" << self_id_
             << " ACCEPTED value seq:" << seq
             << " — sending ACCEPTED to leader:" << leader_id;

  // Send ACCEPTED back to leader
  Request accepted;
  accepted.set_type(Request::TYPE_PAXOS_ACCEPTED);
  accepted.set_seq(seq);
  accepted.set_sender_id(self_id_);
  accepted.set_current_view(proposal_num);
  replica_communicator_->SendMessage(accepted, leader_id);

  return 0;
}

// ── Leader: collect PROMISE ───────────────────────────────────────────────────

int PaxosManager::ProcessPromise(std::unique_ptr<Request> request) {
  uint64_t seq    = request->seq();
  int      sender = request->sender_id();

  LOG(ERROR) << "[Paxos] Leader node:" << self_id_
             << " received PROMISE from node:" << sender
             << " seq:" << seq;

  std::shared_ptr<RoundState> state;
  {
    std::lock_guard<std::mutex> lk(round_mu_);
    auto it = rounds_.find(seq);
    if (it == rounds_.end()) {
      LOG(ERROR) << "[Paxos] No round state for seq:" << seq;
      return 0;
    }
    state = it->second;
    state->promise_count++;
    LOG(ERROR) << "[Paxos] Promise count shard:" << shard_id_
               << " seq:" << seq
               << " = " << state->promise_count
               << "/" << QuorumSize();
  }
  state->cv_promise.notify_all();
  return 0;
}

// ── Leader: collect ACCEPTED ──────────────────────────────────────────────────

int PaxosManager::ProcessAccepted(std::unique_ptr<Request> request) {
  uint64_t seq    = request->seq();
  int      sender = request->sender_id();

  LOG(ERROR) << "[Paxos] Leader node:" << self_id_
             << " received ACCEPTED from node:" << sender
             << " seq:" << seq;

  std::shared_ptr<RoundState> state;
  {
    std::lock_guard<std::mutex> lk(round_mu_);
    auto it = rounds_.find(seq);
    if (it == rounds_.end()) {
      LOG(ERROR) << "[Paxos] No round state for seq:" << seq;
      return 0;
    }
    state = it->second;
    state->accepted_count++;
    LOG(ERROR) << "[Paxos] Accepted count shard:" << shard_id_
               << " seq:" << seq
               << " = " << state->accepted_count
               << "/" << QuorumSize();
  }
  state->cv_accepted.notify_all();
  return 0;
}

}  // namespace resdb
