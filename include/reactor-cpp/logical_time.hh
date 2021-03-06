/*
 * Copyright (C) 2019 TU Dresden
 * All rights reserved.
 *
 * Authors:
 *   Christian Menard
 */

#pragma once

#include "time.hh"

namespace reactor {

using mstep_t = unsigned long;  // at least 32 bit

class LogicalTime;

class Tag {
 private:
  const TimePoint _time_point;
  const mstep_t _micro_step;

  Tag(const TimePoint& time_point, const mstep_t& micro_step)
      : _time_point{time_point}, _micro_step{micro_step} {}

 public:
  // no default constructor, not assignable, but movable and copyable
  Tag() = delete;
  Tag& operator=(const Tag&) = delete;
  Tag(Tag&&) = default;
  Tag(const Tag&) = default;

  const TimePoint& time_point() const { return _time_point; }
  const mstep_t& micro_step() const { return _micro_step; }

  static Tag from_physical_time(TimePoint time_point);
  static Tag from_logical_time(const LogicalTime& lt);

  Tag delay(Duration offset = Duration::zero()) const;
};

// define all the comparison operators
bool operator==(const Tag& lhs, const Tag& rhs);
bool inline operator!=(const Tag& lhs, const Tag& rhs) { return !(lhs == rhs); }
bool operator<(const Tag& lhs, const Tag& rhs);
bool inline operator>(const Tag& lhs, const Tag& rhs) { return rhs < lhs; }
bool inline operator<=(const Tag& lhs, const Tag& rhs) { return !(lhs > rhs); }
bool inline operator>=(const Tag& lhs, const Tag& rhs) { return !(lhs < rhs); }

class LogicalTime {
 private:
  TimePoint _time_point{};
  mstep_t _micro_step{0};

 public:
  void advance_to(const Tag& tag);

  const TimePoint& time_point() const { return _time_point; }
  const mstep_t& micro_step() const { return _micro_step; }
};

bool operator==(const LogicalTime& lt, const Tag& t);
bool inline operator!=(const LogicalTime& lt, const Tag& t) {
  return !(lt == t);
}
bool operator<(const LogicalTime& lt, const Tag& t);
bool operator>(const LogicalTime& lt, const Tag& t);
bool inline operator<=(const LogicalTime& lt, const Tag& t) {
  return !(lt > t);
}
bool inline operator>=(const LogicalTime& lt, const Tag& t) {
  return !(lt < t);
}

bool inline operator==(const Tag& t, const LogicalTime& lt) { return lt == t; }
bool inline operator!=(const Tag& t, const LogicalTime& lt) {
  return !(lt == t);
}
bool inline operator<(const Tag& t, const LogicalTime& lt) { return lt > t; }
bool inline operator>(const Tag& t, const LogicalTime& lt) { return lt < t; }
bool inline operator<=(const Tag& t, const LogicalTime& lt) {
  return !(t > lt);
}
bool inline operator>=(const Tag& t, const LogicalTime& lt) {
  return !(t < lt);
}

}  // namespace reactor
