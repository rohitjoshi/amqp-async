
local lib_path = "libamqp_async.dylib"
local amqp = require("amqp"):new(lib_path)

local uri = "amqp://guest:guest@localhost:5672/"
local exchange = "mobile-activity.in.exchange"
local routing_key = "mobile-activity.in.routing-key"
local log_dir = "/tmp"
local logfile_prefix = "amqp-async"
local log_level = "info"

local sleep = function(n)
	if ngx then
		ngx.sleep(n)
	else
		os.execute("sleep " .. tonumber(n))
	end
end



local ok, err = amqp:init(uri, exchange, routing_key, log_dir, logfile_prefix, log_level)
if not ok then
 print("Failed to initialize. " .. err)
 return
end



for i=1,100000 do
	local ok, err = amqp:publish("test message:" .. i)
        if err then
           print("Failed to publish message");
        else
         print("Published test message:" .. i)
         sleep(0.01)
       end
end
sleep(5)

for i=1,100000 do
	local ok, err = amqp:publish("test message:" .. i)
	sleep(0.01)
        if err then
           print("Failed to publish message");
        end
end

sleep(300)

amqp:stop()


