/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   connection.h
 * Author: Rohit Joshi <rohit.c.joshi@gmail.com>
 *
 * Created on October 27, 2017, 11:10 PM
 */

#ifndef AMQP_CONNECTION_H
#define AMQP_CONNECTION_H

#include <cstring>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

class amqp_connection {

private:
    
    amqp_connection(const amqp_connection& orig);
public:
    amqp_connection(std::string logger_name):_logger_name(logger_name), _socket(NULL),_channel_max(200),_frame_max(131072),_heartbeat(10),
        _method(AMQP_SASL_METHOD_PLAIN),_channel(1){
        _props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        _props.content_type = amqp_cstring_bytes("text/plain");
        _props.delivery_mode = 2;
        
    }
    
    int init(std::string& uri, std::string& exchange, std::string& routing_key, char* error_msg) {
        
         _exchange = exchange;
        _routing_key = routing_key;
        _uri = uri;
      
        spdlog::get(_logger_name)->debug("Parse uri:{}", _uri.c_str());
        int status = amqp_parse_url((char*)_uri.c_str(), &_ci);
        if (status) {
             spdlog::get(_logger_name)->error("Failed to parse uri:{}", _uri.c_str());
             strcpy(error_msg, "Failed to parse url");
            return -1;
        }
        spdlog::get(_logger_name)->debug("vhost:{}, host: {}, Port:{}, user:{}, password:{}", _ci.vhost,_ci.host, _ci.port, _ci.user, _ci.password);
            
        _conn = amqp_new_connection();
        _socket = amqp_tcp_socket_new(_conn);
        if(!_socket) {
            spdlog::get(_logger_name)->error("Failed  to create a tcp socke");
            strcpy(error_msg, "Failed  to create a tcp socket");
            return -1;
        }
        status = amqp_socket_open(_socket, _ci.host, _ci.port);
        if (status) {
            spdlog::get(_logger_name)->error("Failed to connect to host: {}, Port:{}", _ci.host, _ci.port);
            sprintf(error_msg, "Failed to connect to host: %s, Port:%d\n", _ci.host, _ci.port);
            return -1;
        }
        spdlog::get(_logger_name)->debug("Login: vhost:{}, channel_max:{}, _frame_max:{}, _heartbeat:{}, _method:{}, user:{}",
                _ci.vhost, _channel_max, _frame_max, _heartbeat, _method, _ci.user);
        amqp_rpc_reply_t reply = amqp_login(_conn, (_ci.vhost && strlen(_ci.vhost)>0)?_ci.vhost:"/", _channel_max, _frame_max, _heartbeat, _method, _ci.user, _ci.password);
        if(get_amqp_error(reply, (char*)"Logging in", error_msg)){
            return -1;
        }
        spdlog::get(_logger_name)->info("Login successful. Opening a channel");
        
        amqp_channel_open(_conn, _channel);
        reply = amqp_get_rpc_reply(_conn);
        if(get_amqp_error(reply, (char*)"Opening channel", error_msg)){
            return -1;
        }
        spdlog::get(_logger_name)->info("Successfully opened channel");
       
        
        spdlog::get(_logger_name)->debug("_exchange:{},_routing_key:{}", _exchange.c_str(), _routing_key.c_str());
        return 0;
        
    }
    
    int publish(const char* message, char* error_msg) {
        spdlog::get(_logger_name)->debug("Publishing message: {}", message);
        amqp_bytes_t body_bytes = amqp_cstring_bytes(message);
        const char* context = "Publishing Message";
        spdlog::get(_logger_name)->trace("Publishing exchange: {}, routing_key:{}", _exchange.c_str(), _routing_key.c_str());
        int status = amqp_basic_publish(_conn, _channel, amqp_cstring_bytes(_exchange.c_str()),
                amqp_cstring_bytes(_routing_key.c_str()), 0, 0, NULL, body_bytes);
        
        spdlog::get(_logger_name)->trace("publish status: {}", status);
        if (get_error(status,context, error_msg)) {
            spdlog::get(_logger_name)->error("Failed to publish message");
            return -1;
        }
        spdlog::get(_logger_name)->trace("successfully published");
        return body_bytes.len;
    }
    
    int send_heartbeat(char* error_msg) {
        const char* context = "Sending heartbeat";
        amqp_frame_t hb;
        hb.channel = 0;
        hb.frame_type = AMQP_FRAME_HEARTBEAT;
        
        int status = amqp_send_frame(_conn, &hb);
        if (get_error(status,context, error_msg)) {
            return -1;
        }
        return 0;
    }
    
        
        
    virtual ~amqp_connection() {
        //spdlog::get(_logger_name)->debug("Closing AMQP Channel");
        amqp_channel_close(_conn, 1, AMQP_REPLY_SUCCESS);
        //spdlog::get(_logger_name)->debug("Closing AMQP Connection");
        amqp_connection_close(_conn, AMQP_REPLY_SUCCESS);
        //spdlog::get(_logger_name)->debug("Destroying AMQP Connection");
        amqp_destroy_connection(_conn);
    }
private:
    
    
    int get_amqp_error(amqp_rpc_reply_t x, const char *context, char* error)
    {
    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL:
        spdlog::get(_logger_name)->debug("AMQP_RESPONSE_NORMAL");
        return 0;

        case AMQP_RESPONSE_NONE:
        sprintf(error, "%s: missing RPC reply type!\n", context);
        spdlog::get(_logger_name)->error("AMQP_RESPONSE_LIBRARY_EXCEPTION: {}", error);
        break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        sprintf(error, "%s: %s\n", context, amqp_error_string2(x.library_error));
        spdlog::get(_logger_name)->error("AMQP_RESPONSE_LIBRARY_EXCEPTION: {}", error);

        break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
            case AMQP_CONNECTION_CLOSE_METHOD: {
                amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
                sprintf(error, "%s: server connection error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                spdlog::get(_logger_name)->debug("AMQP_CONNECTION_CLOSE_METHOD: {}", error);
                break;
            }
            case AMQP_CHANNEL_CLOSE_METHOD: {
                amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
                sprintf(error, "%s: server channel error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                spdlog::get(_logger_name)->error("AMQP_CHANNEL_CLOSE_METHOD: {}", error);
                break;
            }
            default:
            sprintf(error, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
            spdlog::get(_logger_name)->error("default: {}", error);
            break;
        }
        break;
    }

    return 1;
    }
    int get_error(int x, const char* context, char* error)
    {
        if (x < 0) {
        sprintf(error, "%s: %s\n", context, amqp_error_string2(x));
        return 1;
        }
    return 0;
    }
    
    
    amqp_connection_state_t _conn;
    amqp_basic_properties_t _props;
    amqp_socket_t *_socket;
    struct amqp_connection_info _ci;
    unsigned _channel_max;
    unsigned _frame_max;
    unsigned _heartbeat;
    amqp_sasl_method_enum _method;
    amqp_channel_t _channel;
    std::string _exchange;
    std::string _uri;
    std::string _routing_key;
    std::string _logger_name;
    

};

#endif /* AMQP_CONNECTION_H */

