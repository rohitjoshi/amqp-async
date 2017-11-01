# amqp-async
Non Blocking Queue based AMQP Client. Developed for using from Nginx/OpenResty stack.  It depends on [rabbitmq-c](https://github.com/alanxz/rabbitmq-c)

This is a AMQP implementation using q worker queue which allows clients to enque a message without blocking and worker thread will periodically publish to AMQP server.  

This also provide an FFI interface for other language binding.  I have created one for LuaJIT (for nginx/lua) combination.

cd bindings/lua;
luajit amqp_test.lua


C lang : Example:

```
    const char* uri = "amqp://guest:guest@localhost:5672/";
    const char* exchange = "mobile-activity.in.exchange";
    const char* routing_key = "mobile-activity.in.routing-key";
    const char* log_dir = "/tmp";
    const char* logfile_prefix = "amqp-async-test";
    unsigned log_level = 1;
    if (argc > 1) {
        unsigned l = atoi(argv[1]);
        log_level = l;
    }
    char error_msg[256];
    error_msg[0] = '\0';

    if (!init(uri, exchange, routing_key, log_dir, logfile_prefix, (unsigned)log_level, error_msg)) {
        printf("Failed to initialize: %s\n", error_msg);
        return 0;
    }
    char* msg = "test message";
    if (!publish(msg)) {
            printf("Failed to publish message: %u\n", i);
            return 0;
    }
    stop();
```
    
