-- script to requeue expired jobs.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <current_timestamp>
--
-- output:
--     {} or job_discard_list

local prefix = KEYS[1]
local queue_type = KEYS[2]
local current_timestamp = ARGV[1]

-- check if any of the jobs need to be retried
local requeue_job_list = redis.call('ZRANGEBYSCORE', prefix .. ':' .. queue_type .. ':active', 0, current_timestamp)
local job_discard_list = {}
-- iterate over each job and requeue it.
for _, job in pairs(requeue_job_list) do
   local requeue = true
   local queue_id, job_id = job:match("([^,]+):([^,]+)")
   -- check if the job has any pending requeues.
   local requeues_remaining = redis.call('HGET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':requeues_remaining', job_id)
   if requeues_remaining and tonumber(requeues_remaining) > -1 then
      -- finite requeues_remaining. decrement by one and check.
      requeues_remaining = requeues_remaining - 1
      -- update the new requeues_remaining value.
      redis.call('HSET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':requeues_remaining', job_id, requeues_remaining)
      if requeues_remaining == -1 then
         -- discard this job
	 table.insert(job_discard_list, job)
         -- using these flags as Lua doesn't support 'continue'
	 requeue = false
      end
   end
   if requeue == true then
       -- enqueue the job at the front of the job queue
       local job_queue_key = prefix .. ':' .. queue_type .. ':' .. queue_id
       redis.call('LPUSH', job_queue_key, job_id)
       -- check if this is the only job in the job queue
       if redis.call('LLEN', job_queue_key) == 1 then
	  -- default when time keeper does not exist. next ready time is now.
	  local next_ready_time = current_timestamp
	  -- check if the time keeper exists
	  if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':time') == 1 then
	     local last_dequeue_time = tonumber(redis.call('GET', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':time'))
	     local interval = tonumber(redis.call('HGET', prefix .. ':interval', queue_type .. ':' .. queue_id))
	     -- compute next ready time
	     if last_dequeue_time and interval then
		next_ready_time = last_dequeue_time + interval
	     end
	  end
	  -- insert this queue into the ready sorted set.
	  redis.call('ZADD', prefix .. ':' .. queue_type, next_ready_time, queue_id)
	  redis.call('SADD', prefix .. ':ready:queue_type', queue_type)
       end
       -- remove this queue_id & job_id from active sorted set.
       redis.call('ZREM', prefix .. ':' .. queue_type .. ':active', queue_id .. ':' .. job_id)
       -- check if the removed queue_id was the last item in this active set.
       if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':active') ~= 1 then
	  -- the active set does not exist. remove it from the metrics active queue type set.
	  redis.call('SREM', prefix .. ':active:queue_type', queue_type)
       end
   end
end

return job_discard_list
