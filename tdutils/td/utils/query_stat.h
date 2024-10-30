#ifndef _QUERY_STAT_H__
#define _QUERY_STAT_H__

#include <map>
#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <shared_mutex>

enum class StatType {
  Schedule,
  FuncCost,
};

struct TimeStat {
 public:
  std::string tips_;
  StatType stat_type_;
  std::chrono::steady_clock::time_point start_schedule_at_;
  std::chrono::steady_clock::time_point finish_schedule_at_;
  std::chrono::steady_clock::duration execute_cost_;
};

const int64_t INVALID_COUNTER = -1;
const size_t INVALID_INDEX = (size_t)-1;

class ScheduleContext {
 public:
  ScheduleContext() : counter_(INVALID_COUNTER), index_(-1) {
  }
  ScheduleContext(int64_t counter, size_t index) : counter_(counter), index_(index) {
  }
  int64_t counter() const {
    return counter_;
  }
  size_t index() const {
    return index_;
  }

  static ScheduleContext new_only_counter(int64_t counter) {
    return ScheduleContext(counter, INVALID_INDEX);
  }

 private:
  int64_t counter_;
  size_t index_;
};

class QueryStat {
 public:
  ScheduleContext start_schedule(int64_t counter, const char* tips);
  void finish_schedule(const ScheduleContext& sched_ctx);
  void execute_cost(int64_t counter, const char* tips, std::chrono::steady_clock::duration cost);

 private:
  size_t append_stat(int64_t counter, const TimeStat&& ts);
  void update_stat(const ScheduleContext& sched_ctx);

 public:
  void print(int64_t counter);

 private:
  std::shared_mutex mutex_;
  std::map<int64_t, std::vector<TimeStat>> stats_;
};

extern QueryStat g_query_stat;

#endif /*_QUERY_STAT_H__*/