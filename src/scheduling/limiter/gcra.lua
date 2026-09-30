-- One GCRA limiter in Redis, the same algorithm as `Gcra` in limiter.rs.
--
-- Every time is in microseconds of the Redis server's clock (TIME), never of
-- the calling replica's: replicas' clocks drift, and a limit they share needs
-- one clock. The caller turns the wait this script returns into a local
-- instant by adding it to its own "now".
--
-- KEYS[1]  the limiter's key, a hash with two fields:
--            tat    theoretical arrival time: when the bucket would be full again
--            pause  no start before this time (set after a 429), 0 for none
-- ARGV[1]  operation: book, adjust, pause or peek
-- ARGV[2]  emission: microseconds one unit occupies at the sustained rate
-- ARGV[3]  burst: units that may go back to back before pacing starts
-- ARGV[4]  book: cost in units. adjust: units to charge (negative: to give back).
--          pause: microseconds from now until the pause ends.
-- ARGV[5]  book: longest wait to accept in microseconds, -1 for any wait
--
-- Returns {accepted, wait, pause_left}, integers:
--   accepted    1, or 0 when a book was refused because the wait was too long
--   wait        microseconds until a booking may start (book, peek), else 0
--   pause_left  microseconds left of the pause, 0 when none is running

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])

local op = ARGV[1]
local emission = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local amount = tonumber(ARGV[4])

local stored = redis.call('HMGET', KEYS[1], 'tat', 'pause')
-- A missing key is an idle limiter: a full bucket and no pause.
local tat = tonumber(stored[1]) or now
local pause = tonumber(stored[2]) or 0

local capacity = emission * burst

-- The earliest start for `cost` units: once the bucket has room for them, and
-- never during a pause. A cost above the burst waits for a full bucket and
-- then overdraws it.
local function start_for(cost)
  local room_needed = emission * math.min(cost, burst)
  local start = math.max(now, tat + room_needed - capacity)
  return math.max(start, pause)
end

local function pause_left()
  return math.ceil(math.max(pause - now, 0))
end

-- Numbers are written as whole microseconds: Lua's default number-to-string
-- conversion keeps 14 digits, and a time in microseconds has 16.
local function save()
  redis.call('HSET', KEYS[1], 'tat', string.format('%.0f', tat), 'pause', string.format('%.0f', pause))
  -- Once both times are behind us the limiter is idle again, which is the
  -- same as no key at all, so the key can go. A second of margin covers
  -- rounding.
  local ahead = math.max(tat, pause) - now
  redis.call('PEXPIRE', KEYS[1], string.format('%.0f', math.ceil(math.max(ahead, 0) / 1000) + 1000))
end

if op == 'book' then
  local start = start_for(amount)
  local wait = math.ceil(start - now)
  local longest = tonumber(ARGV[5])
  if longest >= 0 and wait > longest then
    -- Books nothing, and writes nothing.
    return {0, wait, pause_left()}
  end
  tat = math.max(tat, start) + emission * amount
  save()
  return {1, wait, pause_left()}
elseif op == 'adjust' then
  -- Giving units back to an idle limiter changes nothing: it is already full.
  if amount > 0 or stored[1] then
    tat = tat + emission * amount
    save()
  end
  return {1, 0, pause_left()}
elseif op == 'pause' then
  local resume = now + amount
  if pause < resume then
    pause = resume
  end
  -- Resume without a burst: the bucket is empty but for one unit.
  tat = math.max(tat, resume + math.max(capacity - emission, 0))
  save()
  return {1, 0, pause_left()}
elseif op == 'peek' then
  return {1, math.ceil(start_for(1) - now), pause_left()}
end

return redis.error_reply('unknown operation ' .. tostring(op))
