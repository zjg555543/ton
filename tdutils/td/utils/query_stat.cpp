#include "query_stat.h"
#include "td/utils/logging.h"
#include <sstream>
#include <mutex>

// #define ENABLE_STATISTICS
QueryStat g_query_stat;

size_t QueryStat::append_stat(int64_t counter, const TimeStat&& ts) {
#ifdef ENABLE_STATISTICS
  std::unique_lock lock(mutex_);

  auto it = this->stats_.find(counter);
  if (it == this->stats_.end()) {
    auto ele = std::vector{ts};
    this->stats_[counter] = ele;
    return 0;
  } else {
    it->second.push_back(ts);
    return it->second.size() - 1;
  }
#else
  return INVALID_INDEX;
#endif  // ENABLE_STATISTICS
}

void QueryStat::update_stat(const ScheduleContext& sched_ctx) {
#ifdef ENABLE_STATISTICS
  const auto finish_schedule_at = std::chrono::steady_clock::now();
  std::unique_lock lock(mutex_);

  auto it = this->stats_.find(sched_ctx.counter());
  if (it == this->stats_.end()) {
    LOG(ERROR) << "unexpect not find counter " << sched_ctx.counter();
    return;
  }
  if (sched_ctx.index() >= it->second.size()) {
    LOG(ERROR) << "unexpect index with counter " << sched_ctx.counter() << ". max size is " << it->second.size()
               << " but context index is " << sched_ctx.index();
    return;
  }

  it->second[sched_ctx.index()].finish_schedule_at_ = finish_schedule_at;
#endif  // ENABLE_STATISTICS
}

ScheduleContext QueryStat::start_schedule(int64_t counter, const char* tips) {
  if (counter == INVALID_COUNTER) {
    return ScheduleContext();
  }

  const TimeStat ts = {.tips_ = std::string(tips),
                       .stat_type_ = StatType::Schedule,
                       .start_schedule_at_ = std::chrono::steady_clock::now()};
  const auto index = this->append_stat(counter, std::move(ts));

  return ScheduleContext(counter, index);
}

void QueryStat::finish_schedule(const ScheduleContext& sched_ctx) {
  if (sched_ctx.counter() == INVALID_COUNTER || sched_ctx.index() == INVALID_INDEX) {
    return;
  }

  this->update_stat(sched_ctx);
}

void QueryStat::execute_cost(int64_t counter, const char* tips, std::chrono::steady_clock::duration cost) {
  const TimeStat ts = {.tips_ = std::string(tips), .stat_type_ = StatType::FuncCost, .execute_cost_ = cost};

  this->append_stat(counter, std::move(ts));
}

void QueryStat::print(int64_t counter) {
#ifdef ENABLE_STATISTICS
  if (counter == INVALID_COUNTER) {
    return;
  }
  std::shared_lock lock(mutex_);

  const auto it = this->stats_.find(counter);
  if (it == this->stats_.end()) {
    LOG(ERROR) << "can not find counter " << counter << "when print";
    return;
  }

  const auto& time_stats = it->second;

  std::stringstream buf;
  // for (int64_t i = 0; i < time_stats.size(); i++) {
  for (const auto& ts : time_stats) {
    if (ts.stat_type_ == StatType::FuncCost) {
      // function call cost
      buf << ts.tips_ << " cost: " << std::chrono::duration_cast<std::chrono::microseconds>(ts.execute_cost_).count()
          << "μs. " << std::endl;
    } else if (ts.stat_type_ == StatType::Schedule) {
      // schedule
      if (ts.finish_schedule_at_ == std::chrono::steady_clock::time_point()) {
        buf << ts.tips_ << " is scheduled but not finish yet. elapsed "
            << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                     ts.start_schedule_at_)
                   .count()
            << "μs" << std::endl;
      } else {
        buf << ts.tips_ << " schedule cost: "
            << std::chrono::duration_cast<std::chrono::microseconds>(ts.finish_schedule_at_ - ts.start_schedule_at_)
                   .count()
            << "μs" << std::endl;
      }
    }
  }

  LOG(WARNING) << "query stat counter:" << counter << ". " << buf.str();
#endif  // ENABLE_STATISTICS
}
