-- script to mark a job as completed (finished) successfully.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <queue_id>
--     ARGV[2] - <job_id>
-- output:
--     nil

local prefix = KEYS[1]
local queue_type = KEYS[2]
local queue_id = ARGV[1]
local job_id = ARGV[2]


-- remove the job from active sorted set.
local response = redis.call('ZREM', prefix .. ':' .. queue_type .. ':active', queue_id .. ':' .. job_id)
if response ~= 1 then
   -- the job was not found in the active sorted set. Non existent job or
   -- the job is expired and was requeued back.
   return 0
end

-- check if the just-removed job was the last job in the active sorted set.
if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':active') ~= 1 then
   -- yes. this was the last job. remove this queue_type
   -- from the metrics active queue type set.
   redis.call('SREM', prefix .. ':active:queue_type', queue_type)
end
-- delete the payload related to this job from the payload map.
redis.call('HDEL', prefix .. ':payload', queue_type .. ':' .. queue_id .. ':' .. job_id)
if redis.call('EXISTS', prefix .. ':' .. queue_type .. ':' .. queue_id) ~= 1 then
   -- there are no more jobs in this queue. we can safely delete the interval.
   redis.call('HDEL', prefix .. ':interval', queue_type .. ':' .. queue_id)
end

-- delete the requeues_remaining entry for this job.
redis.call('HDEL', prefix .. ':' .. queue_type .. ':' .. queue_id .. ':requeues_remaining', job_id)

return 1
