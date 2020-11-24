-- script to enqueue a job into sharq.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <current_timestamp>
--     ARGV[2] - <queue_id>
--     ARGV[3] - <job_id>
--     ARGV[4] - <serialized_payload>
--     ARGV[5] - <interval>
--     ARGV[6] - <requeue_limit>
-- output:
--     nil

local prefix = KEYS[1]
local queue_type = KEYS[2]

local current_timestamp = ARGV[1]
local queue_id = ARGV[2]
local job_id = ARGV[3]
local payload = ARGV[4]
local interval = ARGV[5]
local requeue_limit = ARGV[6]

-- push the job id into the job queue.
redis.call('RPUSH', prefix .. ':' .. queue_type .. ':' .. queue_id, job_id)

-- update the payload map.
redis.call('HSET', prefix .. ':payload', queue_type .. ':' .. queue_id .. ':' .. job_id, payload)

-- update the interval map.
redis.call('HSET', prefix .. ':interval', queue_type .. ':' .. queue_id, interval)

-- update the requeue limit map.
redis.call('HSET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':requeues_remaining', job_id, requeue_limit)

-- check if the queue of this job is already present in the ready sorted set.
if not redis.call('ZRANK', prefix .. ':' .. queue_type, queue_id) then
   -- the ready sorted set is empty, update it and add it to metrics ready queue type set.
   redis.call('SADD', prefix .. ':ready:queue_type', queue_type)
   if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':time') ~= 1 then
      -- time keeper does not exist
      -- update the ready sorted set with current time as ready time.
      redis.call('ZADD', prefix .. ':' .. queue_type, current_timestamp, queue_id)
   else
      -- time keeper exists
      local last_dequeue_time = redis.call('GET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':time')
      local ready_time = interval + last_dequeue_time
      redis.call('ZADD', prefix .. ':' .. queue_type, ready_time, queue_id)
   end
end

-- update the metrics counters
-- update global counter.
local timestamp_minute = math.floor(current_timestamp/60000) * 60000 -- get the epoch for the minute
local expiry_time = math.floor((timestamp_minute + 600000) / 1000) -- store the data for 10 minutes.
if redis.call('EXISTS', prefix .. ':enqueue_counter:' .. timestamp_minute) ~= 1 then
   -- counter does not exists. set the initial value and expiry.
   redis.call('SET', prefix .. ':enqueue_counter:' .. timestamp_minute, 1)
   redis.call('EXPIREAT', prefix .. ':enqueue_counter:' .. timestamp_minute, expiry_time)
else
   -- counter already exists. just increment the value.
   redis.call('INCR', prefix .. ':enqueue_counter:' .. timestamp_minute)
end

-- update the current queue counter.
if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':enqueue_counter:' .. timestamp_minute) ~= 1 then
   -- counter does not exists. set the initial value and expiry.
   redis.call('SET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':enqueue_counter:' .. timestamp_minute, 1)
   redis.call('EXPIREAT', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':enqueue_counter:' .. timestamp_minute, expiry_time)
else
   -- counter already exists. just increment the value.
   redis.call('INCR', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':enqueue_counter:' .. timestamp_minute)
end
