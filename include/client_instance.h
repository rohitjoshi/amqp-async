/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   client_instance.h
 * Author: Rohit Joshi <rohit.c.joshi@gmail.com>
 *
 * Created on October 27, 2017, 10:11 PM
 */

#ifndef CLIENT_INSTANCE_H
#define CLIENT_INSTANCE_H
#include <cstring>
#include <thread>  
#include <chrono>
#include <iostream>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "log.h"
#include "amqp_connection.h"
#include "readerwriterqueue.h"

#define TID pthread_self()
#define PID getpid()

class client_instance {
private:

    client_instance() : _conn(NULL), _stop(false), _thead_sleep_ms(1000),
    _publish_queue(1000), _heartbeat_ms(25000), _logger_name("amqp_async_logger"), _exit(false) {

    }
    client_instance(const client_instance& orig);
public:
    // get singleton instance

    static client_instance& get_instance() {
        static client_instance _instance;
        return _instance;
    }
    //initialize

    static bool init(const char* uri, const char* exchange, const char* routing_key,
            const char* log_dir, const char* logfile_prefix, unsigned log_level) {
        
       
        if (get_instance()._initialized) {
            LOG_RET_TRUE("Already initialized");
        }
        if(!log::init(log_dir, logfile_prefix, (spdlog::level::level_enum)log_level)) {
            printf("Failed to initialize logger\n.");
            return false;
        }
         LOG_IN("uri:%s, exchange:%s, routing_key:%s, log_dir:%s, logfile_prefix:%s, log_level:%d\n", 
                uri, exchange, routing_key, log_dir, logfile_prefix, log_level);

        get_instance()._uri = std::string(uri);
        get_instance()._exchange = std::string(exchange);
        get_instance()._routing_key = std::string(routing_key);
        LOG_TRACE("_exchange:%s,_routing_key:%s",
                get_instance()._exchange.c_str(), get_instance()._routing_key.c_str());
        
        if (!get_instance().connect(false)) {
             LOG_ERROR("Failed to connect to AMQP server. Retrying later. ");
        }

        get_instance()._initialized = true;
        get_instance()._thread = std::thread(
                [&]() {
                    
                    get_instance().run();
                });
         LOG_RET_TRUE("Initialized");
    }
    //publish message

    static bool publish(const char* msg) {
        LOG_IN("msg:%p", msg);
        if (!get_instance()._initialized) {
            LOG_ERROR("You must initialize instance before calling publish");
            LOG_RET_FALSE("You must initialize instance before calling publish ")
        }
        if (get_instance()._stop) {
            LOG_ERROR("Stop function is invoked. No more messages allowed to enqueue");
             LOG_RET_FALSE("Stop function is invoked. No more messages allowed to enqueue ")
        }
        LOG_DEBUG("Enqueue a message:%s", msg);
        if(get_instance()._publish_queue.enqueue(msg)) {
            LOG_RET_TRUE("Successfully enqueued message")
        }else {
            LOG_RET_FALSE("Failed to enqueue message")
        }
    }

    static bool stop() {
        LOG_IN("");
        if (!get_instance()._initialized) {
            LOG_ERROR("You must initialize instance before calling stop");
            LOG_RET_FALSE("You must initialize instance before calling stop")
        }
        LOG_INFO("Stop command executed.");
        get_instance()._stop = true;
        //block the stop function until loop is finished and _exit flag is set to true
        while (!get_instance()._exit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(get_instance()._thead_sleep_ms));
        }
        
        LOG_RET_TRUE("Successfully stopped.");


    }

    bool connect(bool force = false) {
        LOG_IN("force:%d", force);

        // if force initialized, delete existing connection
        if (force) {
            LOG_DEBUG("force is true");
            if (_conn) {
                LOG_DEBUG("deleting connection\n");
                delete _conn;
                _conn = NULL;
            }
        }
        // if already initialized, return success
        if (_conn && _conn->is_connected()) {
            LOG_TRACE("Existing connection. returning true");
            LOG_RET_TRUE("Existing connection. returning true")
        }else if(_conn) {
            LOG_DEBUG("deleting connection\n");
            delete _conn;
            _conn = NULL;
        }
        LOG_DEBUG("creating new connection");
        // create a new connection
        amqp_connection *conn = new amqp_connection(_logger_name);
        LOG_DEBUG("_uri:%s, _exchange:%s,_routing_key:%s", _uri.c_str(), _exchange.c_str(), _routing_key.c_str());

        if(conn->init(_uri, _exchange, _routing_key)) {
           LOG_INFO("Successfully Connected to the Serve\n");
            _conn = conn;
           LOG_RET_TRUE("Successfully Connected to the Serve")
        }
        LOG_ERROR("Failed to connect to server.");
        if (conn) {
            delete conn;
        }
        conn = NULL;
        LOG_RET_FALSE("Failed to create a connection");
    }

    bool amqp_publish(const char* message,  unsigned retry = 3) {
        LOG_IN("message:%s, retry:%p", message, retry);

        bool force = false;
        for (unsigned i = 0; i < retry; ++i) {
            if (!connect(force)) {
                force = true;
                continue;
            }
            if (_conn->publish(message)) {
                LOG_DEBUG("publish successful");
                LOG_RET_TRUE("publish successful");
            }
        }
      //  LOG_ERROR("Failed to publish message.");
        LOG_RET_FALSE("failed to publish message");

    }

    bool send_heartbeat() {
        if (!connect(false)) {
            LOG_RET_FALSE("failed to connect");
        }
        if(_conn->send_heartbeat()) {
            LOG_RET_TRUE("")
        }else {
           if (!connect(true)) {
                LOG_RET_FALSE("failed to connect");
           }
        }
        LOG_RET_FALSE("")
    }

    void run() {
        LOG_IN("");
        LOG_INFO("Starting the AMQP Publish thread loop");
        unsigned counter = 0;
        //send heartbeat every 30 second
        unsigned heartbeat_threshold = _heartbeat_ms / _thead_sleep_ms;
        std::chrono::milliseconds queue_timeout(_thead_sleep_ms);
        bool more_messages = false;
        while (!_stop && !more_messages) {
            std::string item;
            if (_publish_queue.wait_dequeue_timed(item, queue_timeout)) {
                if (!amqp_publish(item.c_str(), 2)) {
                    LOG_ERROR("Failed to publish message. Writing to event log ");
                    LOG_EVENT( item.c_str());
                }else {
                    LOG_INFO("Message published successfully")
                }
                counter = 0;
                more_messages = false;
            } else {
                
                counter++;
                if (counter >= heartbeat_threshold) {
                    LOG_DEBUG("Sending a heartbeat");
                    send_heartbeat();
                    counter = 0;
                    //drain the messages until next heartbeat
                    if (_stop) {
                        more_messages = true;
                    }
                }
               

            }

        }
        LOG_INFO("Exiting the AMQP Publish thread loop");
        _exit = true;
    }

    virtual ~client_instance() {
        LOG_IN("");
       if (get_instance()._thread.joinable()) {
            //LOG_TRACE("Joining Thread");
            get_instance()._thread.join();
        }
        if (get_instance()._conn) {
            //LOG_TRACE("Deleting Connection object");
            delete get_instance()._conn;
            get_instance()._conn = NULL;
        }

        //LOG_TRACE("Client Instance destroyed");
        LOG_OUT("")

    }

    amqp_connection *_conn;
    std::thread _thread;
    bool _initialized;
    std::string _uri;
    std::string _exchange;
    std::string _routing_key;
    bool _stop;
    unsigned _thead_sleep_ms;
    unsigned _heartbeat_ms;
    moodycamel::BlockingReaderWriterQueue<std::string> _publish_queue;
    std::string _logger_name;
    bool _exit = false;

};

#endif /* CLIENT_INSTANCE_H */

