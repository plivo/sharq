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


-- push the job id into the job queue.
redis.call('RPUSH', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2], ARGV[3])

-- update the payload map.
redis.call('HSET', KEYS[1] .. ':payload', KEYS[2] .. ':' .. ARGV[2] .. ':' .. ARGV[3], ARGV[4])

-- update the interval map.
redis.call('HSET', KEYS[1] .. ':interval', KEYS[2] .. ':' .. ARGV[2], ARGV[5])

-- update the requeue limit map.
redis.call('HSET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':requeues_remaining', ARGV[3], ARGV[6])

-- check if the queue of this job is already present in the ready sorted set.
if not redis.call('ZRANK', KEYS[1] .. ':' .. KEYS[2], ARGV[2]) then
   -- the ready sorted set is empty, update it and add it to metrics ready queue type set.
   redis.call('SADD', KEYS[1] .. ':ready:queue_type', KEYS[2])
   if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':time') ~= 1 then
      -- time keeper does not exist
      -- update the ready sorted set with current time as ready time.
      redis.call('ZADD', KEYS[1] .. ':' .. KEYS[2], ARGV[1], ARGV[2])
   else
      -- time keeper exists
      local interval = ARGV[5]
      local last_dequeue_time = redis.call('GET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':time')
      local ready_time = interval + last_dequeue_time
      redis.call('ZADD', KEYS[1] .. ':' .. KEYS[2], ready_time, ARGV[2])
   end
end

-- update the metrics counters
-- update global counter.
local timestamp_minute = math.floor(ARGV[1]/60000) * 60000 -- get the epoch for the minute
local expiry_time = math.floor((timestamp_minute + 600000) / 1000) -- store the data for 10 minutes.
if redis.call('EXISTS', KEYS[1] .. ':enqueue_counter:' .. timestamp_minute) ~= 1 then
   -- counter does not exists. set the initial value and expiry.
   redis.call('SET', KEYS[1] .. ':enqueue_counter:' .. timestamp_minute, 1)
   redis.call('EXPIREAT', KEYS[1] .. ':enqueue_counter:' .. timestamp_minute, expiry_time)
else
   -- counter already exists. just increment the value.
   redis.call('INCR', KEYS[1] .. ':enqueue_counter:' .. timestamp_minute)
end

-- update the current queue counter.
if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':enqueue_counter:' .. timestamp_minute) ~= 1 then
   -- counter does not exists. set the initial value and expiry.
   redis.call('SET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':enqueue_counter:' .. timestamp_minute, 1)
   redis.call('EXPIREAT', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':enqueue_counter:' .. timestamp_minute, expiry_time)
else
   -- counter already exists. just increment the value.
   redis.call('INCR', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[2] .. ':enqueue_counter:' .. timestamp_minute)
end
