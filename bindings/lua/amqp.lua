local ffi = require("ffi")
local ffi_new = ffi.new
local ffi_gc = ffi.gc
local ffi_str = ffi.string
local ffi_copy = ffi.copy
local C = ffi.C




local _M = { _VERSION = '0.01' }
local mt = { __index = _M }

local logging_level = {
    trace    = 0,
    debug    = 1,
    info     = 2,
    notice   = 3,
    warn     = 4,
    err      = 5,
    critical = 6,
    alert    = 7,
    emerg    = 8,
    off      = 9
}


ffi.cdef([[

int init(const uint8_t* uri, const uint8_t* exchange, const uint8_t* routing_key, const uint8_t* log_dir, const uint8_t* logfile_prefix, uint32_t log_level, uint8_t* error_msg);
int publish(const uint8_t* msg);
int stop();

]])

function _M.new(self,lib_path) 
	if not lib_path then
	  local lib_type = "so"
	  if ffi.os == "OSX" then
	  	lib_type = "dylib"
	  end
      lib_path = string.format("libamqp_async.%s", lib_type)
    end
    print("Loading library:" .. lib_path)
    local ctx = ffi.load(lib_path )
    if not ctx then
        error("Failed to  library: " .. lib_path  )
        return nil
    end
     local obj = {
        ctx = ctx
    }
    return setmetatable(obj, mt)
end

function _M:init(uri, exchange, routing_key, log_dir, logfile_prefix, log_level)
	if not uri or not exchange or not routing_key or not log_dir or not logfile_prefix then
		return nil, "One of the parameter is empty"
	end
	
	local uri_buff = ffi_new("unsigned char[?]", #uri)
	ffi.copy(uri_buff, uri, #uri)

	local exchange_buff = ffi_new("unsigned char[?]", #exchange)
	ffi.copy(exchange_buff, exchange, #exchange)
	
	local routing_key_buff = ffi_new("unsigned char[?]", #routing_key)
	ffi.copy(routing_key_buff, routing_key, #routing_key)
	
	local log_dir_buf = ffi_new("unsigned char[?]", #log_dir)
	ffi.copy(log_dir_buf, log_dir, #log_dir)
	
	local logfile_prefix_buf = ffi_new("unsigned char[?]", #logfile_prefix)
	ffi.copy(logfile_prefix_buf, logfile_prefix, #logfile_prefix)
    
        local error_msg = ffi_new("unsigned char[?]", 256)
        local level = logging_level[log_level]
        if not level then
     	  level = logging_level["info"]
        end
        print("Using log level:" .. level)
	local status = self.ctx.init(uri_buff, exchange_buff, routing_key_buff, log_dir_buf, logfile_prefix_buf, level, error_msg)
	if status == 0  then
		return nil,  ffi.string(error_msg, 256)
	end
	return true
end

function _M:publish(message)
	local message_buff = ffi_new("unsigned char[?]", #message+1)
	ffi.copy(message_buff, message, #message)
	local status = self.ctx.publish(message_buff)
	if status == 0   then
		return nil, "Failed to publish message. " 
	end
	return success
end

-- NOTE: this is a blocking call until queue wait time
function _M:stop()
	local status = self.ctx.stop()
        if status == 0  then
		return nil, "Failed to publish message. " 
	end
	return success
end

return _M


