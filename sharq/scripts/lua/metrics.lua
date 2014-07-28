-- script to return a sliding window of rates over the past 10 mins.

-- input:
--     KEYS[1] - <key_prefix>
--
--     ARGV[1] - <current_timestamp>
-- output:
--     { enqueue_response, dequeue_response }

local enqueue_response = {}
local dequeue_response = {}
local timestamp_minute = math.floor(ARGV[1]/60000) * 60000 -- get the epoch for the minute
for i=0, 9 do
   local enqueue_counter_value = redis.call('GET', KEYS[1] .. ':enqueue_counter:' .. timestamp_minute)
   local dequeue_counter_value = redis.call('GET', KEYS[1] .. ':dequeue_counter:' .. timestamp_minute)
   table.insert(enqueue_response, timestamp_minute)
   table.insert(enqueue_response, enqueue_counter_value)
   table.insert(dequeue_response, timestamp_minute)
   table.insert(dequeue_response, dequeue_counter_value)
   timestamp_minute = timestamp_minute - 60000
end

return { enqueue_response, dequeue_response }
