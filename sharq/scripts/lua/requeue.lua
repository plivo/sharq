-- script to requeue expired jobs.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <current_timestamp>
--
-- output:
--     {} or job_discard_list

-- check if any of the jobs need to be retried
local requeue_job_list = redis.call('ZRANGEBYSCORE', KEYS[1] .. ':' .. KEYS[2] .. ':active', 0, ARGV[1])
local job_discard_list = {}
-- iterate over each job and requeue it.
for _, job in pairs(requeue_job_list) do
   local requeue = true
   local queue_id, job_id = job:match("([^,]+):([^,]+)")
   -- check if the job has any pending requeues.
   local requeues_remaining = redis.call('HGET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. queue_id .. ':requeues_remaining', job_id)
   if tonumber(requeues_remaining) > -1 then
      -- finite requeues_remaining. decrement by one and check.
      requeues_remaining = requeues_remaining - 1
      -- update the new requeues_remaining value.
      redis.call('HSET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. queue_id .. ':requeues_remaining', job_id, requeues_remaining)
      if requeues_remaining == -1 then
         -- discard this job
	 table.insert(job_discard_list, job)
         -- using these flags as Lua doesn't support 'continue'
	 requeue = false
      end
   end
   if requeue == true then
       -- enqueue the job at the front of the job queue
       local job_queue_key = KEYS[1] .. ':' .. KEYS[2] .. ':' .. queue_id
       redis.call('LPUSH', job_queue_key, job_id)
       -- check if this is the only job in the job queue
       if redis.call('LLEN', job_queue_key) == 1 then
	  -- check if the time keeper exists
	  local next_ready_time = 0
	  if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':' .. queue_id .. ':time') == 1 then
	     local last_dequeue_time = redis.call('GET', KEYS[1] .. ':' .. KEYS[2] .. ':' .. queue_id .. ':time')
	     local interval = redis.call('HGET', KEYS[1] .. ':interval', KEYS[2] .. ':' .. queue_id)
	     -- compute next ready time
	     next_ready_time = last_dequeue_time + interval
	  else
	     -- time keeper does not exist. next ready time is now.
	     next_ready_time = ARGV[1]
	  end
	  -- insert this queue into the ready sorted set.
	  redis.call('ZADD', KEYS[1] .. ':' .. KEYS[2], next_ready_time, queue_id)
	  redis.call('SADD', KEYS[1] .. ':ready:queue_type', KEYS[2])
       end
       -- remove this queue_id & job_id from active sorted set.
       redis.call('ZREM', KEYS[1] .. ':' .. KEYS[2] .. ':active', queue_id .. ':' .. job_id)
       -- check if the removed queue_id was the last item in this active set.
       if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':active') ~= 1 then
	  -- the active set does not exist. remove it from the metrics active queue type set.
	  redis.call('SREM', KEYS[1] .. ':active:queue_type', KEYS[2])
       end
   end
end

return job_discard_list
