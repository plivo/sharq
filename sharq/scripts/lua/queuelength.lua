-- script to get the queue length.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--     KEYS[3] - <queue_id>
-- output:
--     The current length of the Redis key

local prefix = KEYS[1]
-- local queue_type = KEYS[2]
-- local queue_id = KEYS[3]

-- local key = prefix .. ':' .. queue_type .. ':' .. queue_id

-- local redis_key = prefix

local length = redis.call('LLEN', prefix)

return length
