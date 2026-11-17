ROLES = ('admin', 'operator', 'viewer')
DRILL_STATUSES = ('queued', 'running', 'completed', 'failed', 'canceled', 'expired')
# 活跃状态占用队列名额；终态不再占用且不可撤销
ACTIVE_STATUSES = ('queued', 'running')
TERMINAL_STATUSES = ('completed', 'failed', 'canceled', 'expired')
FINISH_REASONS = ('completed', 'failed', 'user_cancel', 'deadline_expired')
