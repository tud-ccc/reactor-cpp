/*
 * Copyright (C) 2019 TU Dresden
 * All rights reserved.
 *
 * Authors:
 *   Christian Menard
 */

#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "fwd.hh"
#include "logical_time.hh"

namespace reactor {

class Scheduler {
 public:
  using WorkItem = std::packaged_task<void(void)>;
  using WorkPtr = std::unique_ptr<WorkItem>;
  using EventMap = std::unordered_map<BaseAction*, std::function<void(void)>>;

 private:
  const bool using_workers;
  bool terminate{false};
  LogicalTime _logical_time{};

  Environment* _environment;
  std::vector<std::thread> worker_threads;

  std::mutex m_schedule;
  std::unique_lock<std::mutex> schedule_lock{m_schedule, std::defer_lock};
  std::condition_variable cv_schedule;

  std::mutex m_event_queue;
  std::map<Tag, std::unique_ptr<EventMap>> event_queue;
  std::unique_ptr<EventMap> next_microstep_event_map;

  std::vector<BasePort*> set_ports;

  std::mutex m_reaction_queue;
  std::vector<std::vector<Reaction*>> reaction_queue;
  std::vector<Reaction*> ready_reactions;
  std::set<Reaction*> executing_reactions;
  std::condition_variable cv_ready_reactions;
  std::condition_variable cv_done_reactions;

  void work(unsigned id);

  bool next();

  void set_port_helper(BasePort* p);

  std::atomic<bool> _stop{false};

  void dispatch_reactions_to_workers(const std::vector<Reaction*>& reactions);
  void execute_reactions_inline(const std::vector<Reaction*>& reactions);

 public:
  Scheduler(Environment* env);
  ~Scheduler();

  void schedule_sync(const Tag& tag,
                     BaseAction* action,
                     std::function<void(void)> pre_handler);
  void schedule_async(const Tag& tag,
                      BaseAction* action,
                      std::function<void(void)> pre_handler);

  void lock() { schedule_lock.lock(); }
  void unlock() { schedule_lock.unlock(); }

  void set_port(BasePort*);

  const LogicalTime& logical_time() const { return _logical_time; }

  void start();

  void stop();
};

}  // namespace reactor
