#include "platform/consensus/ordering/pbft/two_phase_commit.h"
#include <glog/logging.h>
#include <chrono>

namespace resdb {

TwoPhaseCommit::TwoPhaseCommit(const ResDBConfig& config,
                               ReplicaCommunicator* replica_communicator)
    : config_(config), replica_communicator_(replica_communicator) {
  global_stats_ = Stats::GetGlobalStats();
  total_replicas_ = 0;
  for (const auto& r : config_.GetReplicaInfos()) {
    if (r.id() <= 4) total_replicas_++;
  }
  LOG(ERROR) << "[2PC] Init node:" << config_.GetSelfInfo().id()
             << " replicas:" << total_replicas_;
}

TwoPhaseCommit::~TwoPhaseCommit() {}

bool TwoPhaseCommit::IsCoordinator() const {
  return config_.GetSelfInfo().id() == 1;
}

int TwoPhaseCommit::RunTwoPhaseCommit(const Request& committed_request) {
  uint64_t seq = committed_request.seq();
  LOG(ERROR) << "[2PC] Coordinator starting 2PC for seq:" << seq;

  auto state = std::make_shared<TxnState>();
  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_[seq] = state;
  }

  // ---- Phase 1: Broadcast PREPARE ----
  auto t0 = std::chrono::steady_clock::now();
  BroadcastPrepare(committed_request);
  LOG(ERROR) << "[2PC] Phase 1: PREPARE broadcast seq:" << seq;

  // Wait for all votes
  {
    std::unique_lock<std::mutex> lk(mutex_);
    bool ok = state->cv.wait_for(lk, std::chrono::seconds(10),
                                 [&] { return state->vote_count >= total_replicas_; });
    if (!ok) {
      LOG(ERROR) << "[2PC] TIMEOUT waiting for votes seq:" << seq;
      return -1;
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  long prepare_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  LOG(ERROR) << "[2PC] Phase 1 DONE. All " << total_replicas_
             << " votes received seq:" << seq << " in " << prepare_ms << "ms";

  // ---- Phase 2: Broadcast COMMIT ----
  BroadcastCommit(seq);
  auto t2 = std::chrono::steady_clock::now();
  long commit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
  LOG(ERROR) << "[2PC] Phase 2 DONE. COMMIT broadcast seq:" << seq
             << " commit_ms:" << commit_ms
             << " total_ms:" << (prepare_ms + commit_ms);

  {
    std::lock_guard<std::mutex> lk(mutex_);
    txn_states_.erase(seq);
  }
  return 0;
}

void TwoPhaseCommit::BroadcastPrepare(const Request& request) {
  Request msg;
  msg.set_type(Request::TYPE_2PC_PREPARE);
  msg.set_seq(request.seq());
  msg.set_sender_id(config_.GetSelfInfo().id());
  msg.set_current_view(request.current_view());
  msg.set_data(request.data());
  replica_communicator_->BroadCast(msg);
}

void TwoPhaseCommit::BroadcastCommit(uint64_t seq) {
  Request msg;
  msg.set_type(Request::TYPE_2PC_COMMIT);
  msg.set_seq(seq);
  msg.set_sender_id(config_.GetSelfInfo().id());
  replica_communicator_->BroadCast(msg);
}

int TwoPhaseCommit::ProcessPrepare(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int coordinator_id = request->sender_id();
  LOG(ERROR) << "[2PC] Participant received PREPARE seq:" << seq
             << " from coordinator:" << coordinator_id;

  // Always vote YES (assignment: no aborts)
  Request vote;
  vote.set_type(Request::TYPE_2PC_VOTE);
  vote.set_seq(seq);
  vote.set_sender_id(config_.GetSelfInfo().id());
  vote.set_ret(0);  // YES
  replica_communicator_->SendMessage(vote, coordinator_id);
  LOG(ERROR) << "[2PC] Participant sent VOTE_YES seq:" << seq;
  return 0;
}

int TwoPhaseCommit::ProcessVote(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int sender = request->sender_id();
  LOG(ERROR) << "[2PC] Coordinator received VOTE from node:" << sender
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
               << " = " << state->vote_count << "/" << total_replicas_;
  }
  state->cv.notify_all();
  return 0;
}

int TwoPhaseCommit::ProcessCommit(std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  LOG(ERROR) << "[2PC] Participant received COMMIT seq:" << seq
             << " - transaction committed!";
  return 0;
}

}  // namespace resdb
