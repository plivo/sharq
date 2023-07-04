-- script to get the queue length.

-- input:
--     KEYS[1] - <redis_key>
-- output:
--     The current length of the Redis key

local prefix = KEYS[1]

local length = redis.call('LLEN', prefix)

return length
