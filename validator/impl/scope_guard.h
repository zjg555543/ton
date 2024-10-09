#ifndef __SCOPE_GUARD_HPP__
#define __SCOPE_GUARD_HPP__

#include <functional>

class ScopeGuard {
 public:
  ScopeGuard(std::function<void(void)>& on_scope_exit) {
    this->on_scope_exit = on_scope_exit;
  }
  ScopeGuard(std::function<void(void)>&& on_scope_exit) {
    this->on_scope_exit = std::move(on_scope_exit);
  }

  void dismiss() {
    is_dismiss = true;
  }
  ~ScopeGuard() {
    if (!is_dismiss)
      on_scope_exit();
  }

  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;
  ScopeGuard(ScopeGuard&&) = delete;
  ScopeGuard& operator=(ScopeGuard&&) = delete;

 protected:
  bool is_dismiss = false;
  std::function<void(void)> on_scope_exit;
};

#define SCOPEGUARD_LINENAME_CAT(name, line) name##line
#define SCOPEGUARD_LINENAME(name, line) SCOPEGUARD_LINENAME_CAT(name, line)
#define SCOPE_EXIT(x) ScopeGuard SCOPEGUARD_LINENAME(scope_exit_, __LINE__)((x))

#endif /*__SCOPE_GUARD_HPP__*/