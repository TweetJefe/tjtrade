local key = KEYS [1]
local limit = tonumber(ARGV[1])
local windowSeconds = tonumber(ARGV[2])

local current = tonumber(redis.call('GET', key) or "0")

if current >= limit then
    return 0
else
    redis.call('INCR', key)
    if current == 0 then
        redis.call('EXPIRE', key, windowSeconds)
    end
    return 1
end