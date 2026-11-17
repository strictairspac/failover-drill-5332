import os
DATABASE_URL = os.environ['DATABASE_URL']
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
JWT_SECRET = os.environ['JWT_SECRET']
# 队列治理：活跃演练（queued+running）上限、执行认领心跳超期、截止时间允许的最大跨度
DRILL_MAX_ACTIVE = int(os.getenv('DRILL_MAX_ACTIVE', '20'))
DRILL_CLAIM_TTL_SECONDS = int(os.getenv('DRILL_CLAIM_TTL_SECONDS', '30'))
DRILL_MAX_TTL_SECONDS = int(os.getenv('DRILL_MAX_TTL_SECONDS', str(7 * 24 * 3600)))
