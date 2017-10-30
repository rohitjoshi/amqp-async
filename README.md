# amqp-async
Non Blocking Queue based AMQP Client. Developed for using from Nginx/OpenResty stack

This is a AMQP implementation using q worker queue which allows clients to enque a message without blocking and worker thread will periodically publish to AMQP server.  

This also provide an FFI interface for other language binding.  I have created one for LuaJIT (for nginx/lua) combination.

cd bindings/lua;
luajit amqp_test.lua
