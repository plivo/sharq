-- script to get the queue length.

-- input:
--     KEYS[1] - <redis_key>
-- output:
--     The current length of the Redis key

local prefix = KEYS[1]
local queue_type = KEYS[2]
local queue_id = KEYS[3]

local length = redis.call('LLEN', prefix .. ':' .. queue_type .. ':' .. queue_id)

return length
