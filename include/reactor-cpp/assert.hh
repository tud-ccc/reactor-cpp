/*
 * Copyright (C) 2019 TU Dresden
 * All rights reserved.
 *
 * Authors:
 *   Christian Menard
 */

#pragma once

// A custom definition of assert that mitigates unused variable warnings
// when assertions are disabled
#ifdef NDEBUG
#define ASSERT(x)    \
  do {               \
    (void)sizeof(x); \
  } while (0)
#else
#include <cassert>
#define ASSERT(x) assert(x)
#endif

#include <sstream>
#include <stdexcept>
#include <string>

namespace reactor {

class ValidationError : public std::runtime_error {
 private:
  static std::string build_message(const std::string& msg);

 public:
  ValidationError(const std::string& msg)
      : std::runtime_error(build_message(msg)) {}
};

void inline validate(bool condition, const std::string& message) {
  if (!condition) {
    throw ValidationError(message);
  }
}


#ifdef REACTOR_CPP_VALIDATE
#define VALIDATE(con, msg) reactor::validate(con, msg);
#else
#define VALIDATE(con, msg) \
  do {               \
    (void)sizeof(con); \
  } while (0)
#endif

}  // namespace reactor
