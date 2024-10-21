/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2020 Telegram Systems LLP
*/
#include "td/actor/core/CpuWorker.h"

#include "td/actor/core/ActorExecutor.h"
#include "td/actor/core/SchedulerContext.h"

#include "td/actor/core/Scheduler.h"  // FIXME: afer LocalQueue is in a separate file
#include "td/utils/logging.h"

namespace td {
namespace actor {
namespace core {
void CpuWorker::run() {
  auto thread_id = get_thread_id();
  auto &dispatcher = *SchedulerContext::get();
  LOG(DEBUG) << "yus start work " << thread_id;

  MpmcWaiter::Slot slot;
  waiter_.init_slot(slot, thread_id);
  auto &debug = dispatcher.get_debug();
  LOG(DEBUG) << "yus  thread_id" << thread_id << "worker id " << this->id_;
  while (true) {
    SchedulerMessage message;
    if (try_pop(message, thread_id)) {
      waiter_.stop_wait(slot);
      if (!message) {
        LOG(DEBUG) << "yus no message return" << " thread_id " << thread_id;
        return;
      }
      auto lock = debug.start(message->get_name());
      LOG(DEBUG) << "yus actor " << message->get_name() << " " << " mailbox number "
                << message->mailbox().reader().calc_size();
      ActorExecutor executor(*message, dispatcher, ActorExecutor::Options().with_from_queue());
    } else {
      LOG(DEBUG) << "yus no message, IDLE";
      waiter_.wait(slot);
    }
  }
}

bool CpuWorker::try_pop_local(SchedulerMessage &message) {
  SchedulerMessage::Raw *raw_message;
  if (local_queues_[id_].try_pop(raw_message)) {
    message = SchedulerMessage(SchedulerMessage::acquire_t{}, raw_message);
    LOG(DEBUG) << "yus " << message->get_name() << "from local queue_" << &local_queues_[id_];
    return true;
  }
  return false;
}

bool CpuWorker::try_pop_global(SchedulerMessage &message, size_t thread_id) {
  SchedulerMessage::Raw *raw_message;
  if (queue_.try_pop(raw_message, thread_id)) {
    message = SchedulerMessage(SchedulerMessage::acquire_t{}, raw_message);
    LOG(DEBUG) << "yus " << message->get_name() << "from global queue_ ";
    return true;
  }
  return false;
}

bool CpuWorker::try_pop(SchedulerMessage &message, size_t thread_id) {
  if (++cnt_ == 51) {
    cnt_ = 0;
    if (try_pop_global(message, thread_id) || try_pop_local(message)) {
      return true;
    }
  } else {
    if (try_pop_local(message) || try_pop_global(message, thread_id)) {
      return true;
    }
  }

  LOG(DEBUG) << "yus " << thread_id << " try to steal";
  for (size_t i = 1; i < local_queues_.size(); i++) {
    size_t pos = (i + id_) % local_queues_.size();
    SchedulerMessage::Raw *raw_message;
    if (local_queues_[id_].steal(raw_message, local_queues_[pos])) {
      message = SchedulerMessage(SchedulerMessage::acquire_t{}, raw_message);

      LOG(DEBUG) << "yus " << thread_id << "local queue id " << id_ << " steal from " << pos << " " << " name "
                << message->get_name();
      return true;
    }
  }

  return false;
}

}  // namespace core
}  // namespace actor
}  // namespace td
