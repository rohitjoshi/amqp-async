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
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "spdlog/spdlog.h"
#include "amqp_connection.h"
#include "readerwriterqueue.h"

#define TID pthread_self()
#define PID getpid()

class client_instance {

private:
    client_instance():_conn(NULL), _stop(false),_thead_sleep_ms(1000),
            _publish_queue(1000),_heartbeat_ms(30000),_exit(false) {
        
    }
    client_instance(const client_instance& orig);
public:
    // get singleton instance
    static client_instance& get_instance() {
        static client_instance _instance;
        return _instance;
    }
    //initialize
    static int init(const char* uri, const char* exchange, const char* routing_key, 
                const char* log_dir, const char* logfile_prefix, unsigned log_level, char* error_msg) {
        
        if(get_instance()._initialized) {
            return 0;
        }
        
        
        get_instance().init_logger (log_dir, logfile_prefix, log_level);
       
        get_instance()._uri = uri;
        get_instance()._exchange = std::string(exchange);
        get_instance()._routing_key = std::string(routing_key);
        //spdlog::get(get_instance()._logger_name)->debug("_exchange:{},_routing_key:{}", get_instance()._exchange.c_str(), get_instance()._routing_key.c_str());

        if(!get_instance().connect(error_msg, false)) {
            return -1;
        }
        
        get_instance()._initialized = true;
        
        
        
        get_instance()._thread = std::thread(
              [&]() {
                  get_instance().run();
              });
        return 0;
    }
    //publish message
    static bool publish(const char* msg) {
        if (!get_instance()._initialized) {
            return false;
        }
        if (get_instance()._stop) {
             spdlog::get(get_instance()._logger_name)->error("Stop function is invoked. No more messages allowed to enqueue");
            return false;
        }
        spdlog::get(get_instance()._logger_name)->debug("Enqueue a message:{}", msg);
        return get_instance()._publish_queue.enqueue(msg);
    }
    
    static void stop() {
        if (!get_instance()._initialized) {
            return;
        }
        spdlog::get(get_instance()._logger_name)->info("Stop command executed.");
        get_instance()._stop = true; 
        //block the stop function until loop is finished and _exit flag is set to true
        while(!get_instance()._exit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(get_instance()._thead_sleep_ms));
        }
         
        
    }
    
    void init_logger(const char* log_dir, const char* logfile_prefix, unsigned log_level) {
        std::string logfile(log_dir);
        logfile.append("/");
        logfile.append(logfile_prefix);
        logfile.append("-");
        logfile.append(std::to_string(PID));
        logfile.append(".log");
        printf("Creating a logger with file:%s\n", logfile.c_str());
        //auto logger = spdlog::stdout_color_mt(get_instance()._logger_name);
        auto logger = spdlog::rotating_logger_mt(get_instance()._logger_name, logfile, 1048576 * 500, 10);
        logger->set_level(spdlog::level::level_enum(log_level));
    }
    
    
    
    
    bool connect(char* error_msg ,bool force=false) {
        // if force initialized, delete existing connection
        if (force) {
            spdlog::get(_logger_name)->debug("force is true");
            if(_conn) {
                spdlog::get(_logger_name)->debug("deleting connection\n");
                delete _conn;
                _conn = NULL;
            }
        }
        // if already initialized, return success
        if (_conn) {
            spdlog::get(_logger_name)->trace("Existing connection. returning true");
            return true;
        }
        spdlog::get(_logger_name)->debug("creating new connection");
        // create a new connection
        amqp_connection *conn =  new amqp_connection(_logger_name);
        spdlog::get(get_instance()._logger_name)->debug("_uri:{}, _exchange:{},_routing_key:{}", _uri.c_str(),_exchange.c_str(), _routing_key.c_str());
        
        int status = conn->init(_uri, _exchange, _routing_key, error_msg);
        if (!status) {
             spdlog::get(_logger_name)->info("Successfully Connected to the Serve\n"); 
            _conn = conn;
            return true;
        }
        spdlog::get(_logger_name)->error("Failed to connect to server. Error:{}", error_msg); 
        if(conn) {
            delete conn;
        }
        conn = NULL;
        return false;
    }
    
    
    
    bool amqp_publish(const char* message, char * error_msg, unsigned retry=3) {
        bool force = false;
        for(unsigned i=0; i<retry; ++i) {
            if(!connect(error_msg, force)) {
                force = true;
                continue;
            }
           if(_conn->publish(message, error_msg)) {
               return true;
           }
        }
        return false;
        
    }
    
    bool send_heartbeat(char * error_msg) {
        if(!connect(error_msg, false)) {
            return false;
        }
        return _conn->send_heartbeat(error_msg);
    }
    
    void run() {
        spdlog::get(_logger_name)->info("Starting the AMQP Publish thread loop");
        char error_msg[256];
        unsigned counter = 0;
        //send heartbeat every 30 second
        unsigned heartbeat_threshold = _heartbeat_ms/_thead_sleep_ms;
        std::chrono::milliseconds queue_timeout(_thead_sleep_ms);
        bool no_more_messages = false;
        while(!_stop && no_more_messages) {
            error_msg[0] = '\0';
            std::string item;
            if (_publish_queue.wait_dequeue_timed(item, queue_timeout)) {
                if(!amqp_publish(item.c_str(), error_msg, 3)) {
                    spdlog::get(_logger_name)->error("Failed to publish message. Error:%s\n",error_msg); 
                    spdlog::get(_logger_name)->info("Message:%s\n", item.c_str());
                }
                counter = 0;
                no_more_messages = false;
            }else {
                counter++;
                if (counter >= heartbeat_threshold) {
                    spdlog::get(_logger_name)->debug("Sending a heartbeat");
                    send_heartbeat(error_msg);
                    counter = 0;
                    //drain the messages until next heartbeat
                    if(_stop) {
                        no_more_messages = true;
                    }
                }
                
            }
            
        }
        spdlog::get(_logger_name)->info("Exiting the AMQP Publish thread loop");
        spdlog::get(_logger_name)->flush();
        _exit= true;
    }
        
        
    virtual ~client_instance() {
        if (get_instance()._thread.joinable()) {
            //spdlog::get(_logger_name)->debug("Joining Thread");
              get_instance()._thread.join();
        }
        if (get_instance()._conn) {
            //spdlog::get(_logger_name)->debug("Deleting Connection object");
            delete get_instance()._conn;
            get_instance()._conn= NULL;
        }
        
        //spdlog::get(_logger_name)->debug("Client Instance destroyed");
        //spdlog::get(_logger_name)->flush();
        
    }

    amqp_connection *_conn;
    std::thread _thread;
    bool _initialized;
    std::string _uri;
    std::string _exchange;
    std::string _routing_key;
    bool _stop;
    unsigned  _thead_sleep_ms;
    unsigned _heartbeat_ms;
    moodycamel::BlockingReaderWriterQueue<std::string> _publish_queue; 
    std::string _logger_name;
    bool _exit = false;

};

#endif /* CLIENT_INSTANCE_H */

