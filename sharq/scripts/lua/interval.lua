-- script to update the interval for a queue only if it exists.

-- input:
--     KEYS[1] - <hashmap_key>
--     KEYS[2] - <queue_key>
--
--     ARGV[1] - <interval>

if redis.call('HEXISTS', KEYS[1], KEYS[2]) ~= 1 then
   -- interval does not exist
   return 0
else
   -- interval exists. update the time.
   redis.call('HSET', KEYS[1], KEYS[2], ARGV[1])
   return 1
end
