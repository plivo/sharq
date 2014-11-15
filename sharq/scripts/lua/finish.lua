-- script to mark a job as completed (finished) successfully.

-- input:
--     KEYS[1] - <key_prefix>
--     KEYS[2] - <queue_type>
--
--     ARGV[1] - <queue_id>
--     ARGV[2] - <job_id>
-- output:
--     nil


-- remove the job from active sorted set.
local response = redis.call('ZREM', KEYS[1] .. ':' .. KEYS[2] .. ':active', ARGV[1] .. ':' .. ARGV[2])
if response ~= 1 then
   -- the job was not found in the active sorted set. Non existent job or
   -- the job is expired and was requeued back.
   return 0
end

-- check if the just-removed job was the last job in the active sorted set.
if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':active') ~= 1 then
   -- yes. this was the last job. remove this queue_type
   -- from the metrics active queue type set.
   redis.call('SREM', KEYS[1] .. ':active:queue_type', KEYS[2])
end
-- delete the payload related to this job from the payload map.
redis.call('HDEL', KEYS[1] .. ':payload', KEYS[2] .. ':' .. ARGV[1] .. ':' .. ARGV[2])
if redis.call('EXISTS', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[1]) ~= 1 then
   -- there are no more jobs in this queue. we can safely delete the interval.
   redis.call('HDEL', KEYS[1] .. ':interval', KEYS[2] .. ':' .. ARGV[1])
end

-- delete the requeues_remaining entry for this job.
redis.call('HDEL', KEYS[1] .. ':' .. KEYS[2] .. ':' .. ARGV[1] .. ':requeues_remaining', ARGV[2])

return 1
