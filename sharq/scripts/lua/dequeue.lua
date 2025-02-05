-- script to dequeue a job from sharq.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <current_timestamp>
--     ARGV[2] - <job_expiry_interval>
-- output:
--     { queue_id, job_id, payload, requeues_remaining }


local prefix = KEYS[1]
local queue_type = KEYS[2]

local current_timestamp = ARGV[1]
local job_expiry_interval = ARGV[2]


local ready_queue_id_list = redis.call('ZRANGEBYSCORE', prefix .. ':' .. queue_type, 0, current_timestamp)
if next(ready_queue_id_list) ~= nil then
   -- there is a queue ready to be dequeued.
   local ready_queue_id = ready_queue_id_list[1]
   -- dequeue a job from the job queue.
   local job_id = redis.call('LPOP', prefix .. ':' .. queue_type .. ':' .. ready_queue_id)
   -- get the payload for this job
   local payload = redis.call('HGET', prefix .. ':payload', queue_type .. ':' .. ready_queue_id .. ':' .. job_id)
   -- update the time keeper with the current dequeue time.
   redis.call('PSETEX', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':time', job_expiry_interval, current_timestamp)
   -- check if there are any more jobs of this queue in the job queue.
   if redis.call('LLEN', prefix .. ':' .. queue_type .. ':' .. ready_queue_id) == 0 then
      -- there are no more jobs of this queue. remove this queue from the ready sorted set.
      redis.call('ZREM', prefix .. ':' .. queue_type, ready_queue_id)
      -- now check if the ready sorted set is empty.
      if redis.call('EXISTS', prefix .. ':' .. queue_type) ~= 1 then
	 -- the ready sorted set is empty. remove this 'queue_type' from
	 -- the metris ready queue type set
	 redis.call('SREM', prefix .. ':ready:queue_type', queue_type)
      end
   else
      -- there are more jobs in the queue. update the next
      -- dequeue time for this queue in the ready sorted set.
      local next_dequeue_time = current_timestamp
      local interval = tonumber(redis.call('HGET', prefix .. ':interval', queue_type .. ':' .. ready_queue_id))
      if interval then
	 next_dequeue_time = current_timestamp + interval
      end
      redis.call('ZADD', prefix .. ':' .. queue_type, next_dequeue_time, ready_queue_id)
   end
   local job_expiry_time = current_timestamp + job_expiry_interval
   -- finally, add the job_id and queue_id that was dequeued into the active sorted set.
   redis.call('ZADD', prefix .. ':' .. queue_type .. ':active', job_expiry_time, ready_queue_id .. ':' .. job_id)
   -- add the queue_type to metrics active queue type set.
   redis.call('SADD', prefix .. ':active:queue_type', queue_type)

   -- get the requeues_remaining for this job
   local requeues_remaining = redis.call('HGET', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':requeues_remaining', job_id)

   -- update the metrics counters
   -- update global counter.
   local timestamp_minute = math.floor(current_timestamp/60000) * 60000 -- get the epoch for the minute
   local expiry_time = math.floor((timestamp_minute + 600000) / 1000) -- store the data for 10 minutes.
   if redis.call('EXISTS', prefix .. ':dequeue_counter:' .. timestamp_minute) ~= 1 then
      -- counter does not exists. set the initial value and expiry.
      redis.call('SET', prefix .. ':dequeue_counter:' .. timestamp_minute, 1)
      redis.call('EXPIREAT', prefix .. ':dequeue_counter:' .. timestamp_minute, expiry_time)
   else
      -- counter already exists. just increment the value.
      redis.call('INCR', prefix .. ':dequeue_counter:' .. timestamp_minute)
   end

   -- update the current queue counter.
   if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':dequeue_counter:' .. timestamp_minute) ~= 1 then
      -- counter does not exists. set the initial value and expiry.
      redis.call('SET', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':dequeue_counter:' .. timestamp_minute, 1)
      redis.call('EXPIREAT', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':dequeue_counter:' .. timestamp_minute, expiry_time)
   else
      -- counter already exists. just increment the value.
      redis.call('INCR', prefix .. ':' .. queue_type .. ':' .. ready_queue_id .. ':dequeue_counter:' .. timestamp_minute)
   end

   return { ready_queue_id, job_id, payload, requeues_remaining }
else
   return { }
end
