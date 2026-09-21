local balanceKey = KEYS[1]
local reservedKey = KEYS[2]
local amount = tonumber(ARGV[1])

local currentBalanceStr = redis.call('GET', balanceKey)
local currentBalance = 0

if currentBalanceStr then
    currentBalance = tonumber(currentBalanceStr)
end

if currentBalance >= amount then
    redis.call('INCRBYFLOAT', balanceKey, -amount)
    redis.call('INCRBYFLOAT', reservedKey, +amount)

    return  1 -- 1
else
    return 0 -- 0
end