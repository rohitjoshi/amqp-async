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

    amqp_connection(std::string logger_name) : _conn(NULL), _connected(false), _channel_max(200), _frame_max(131072), _heartbeat(10),
    _method(AMQP_SASL_METHOD_PLAIN), _channel(1) {
        _props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        _props.content_type = amqp_cstring_bytes("text/plain");
        _props.delivery_mode = 2;

    }

    bool init(std::string& uri, std::string& exchange, std::string& routing_key) {
        LOG_IN("uri:%s, exchange:%s, routing_key:%s", uri.c_str(), exchange.c_str(), routing_key.c_str());
        _exchange = exchange;
        _routing_key = routing_key;
        _uri = uri;
        
        char uri_buffer[256];
        strcpy(uri_buffer, _uri.c_str());

        LOG_DEBUG("Parse uri:%s", uri_buffer);
        int status = amqp_parse_url((char*) uri_buffer, &_ci);
        if (status) {
            LOG_ERROR("Failed to parse uri:%s", _uri.c_str());
            LOG_RET_FALSE("Failed to parse uri.")
        }
        
        amqp_connection_state_t conn = amqp_new_connection();
        if (!conn) {
            LOG_ERROR("Failed  to create a amqp_new_connection ");
            LOG_RET_FALSE("Failed  to create a amqp_new_connection.");
        }
        amqp_socket_t* socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            LOG_ERROR("Failed  to create a tcp socket");
            socket = NULL;
            LOG_RET_FALSE("Failed  to create a tcp socket.");
        }
        
        //non-blocking connection
        struct timeval tval;
        tval.tv_sec = 10;
        tval.tv_usec = 0;
        //status = amqp_socket_open_noblock(socket,_ci.host, _ci.port, &tval);
        status = amqp_socket_open(socket, _ci.host, _ci.port);
        if (status) {
            LOG_CRITICAL("Failed to connect to host: %s, Port:%d", _ci.host, _ci.port);
            LOG_RET_FALSE("Failed to connect to host.");
        }
        LOG_DEBUG("Login: vhost:%s, channel_max:%u, _frame_max:%u, _heartbeat:%u, _method:%d, user:%s",
                _ci.vhost, _channel_max, _frame_max, _heartbeat, _method, _ci.user);
        
        amqp_rpc_reply_t reply = amqp_login(conn, (_ci.vhost && strlen(_ci.vhost) > 0) ? _ci.vhost : "/", _channel_max, _frame_max, _heartbeat, _method, _ci.user, _ci.password);
        if (!log_amqp_error(reply, (char*) "Logging in")) {
            LOG_RET_FALSE("Failed to Login.");
        }
        LOG_INFO("Login successful. Opening a channel");

        amqp_channel_open(conn, _channel);
        reply = amqp_get_rpc_reply(conn);
        if (!log_amqp_error(reply, (char*) "Opening channel")) {
            LOG_RET_FALSE("Failed to open a channel.");
        }
        
        _conn = conn;
        _connected = true;
        LOG_INFO("Successfully opened channel");
        LOG_RET_TRUE("success");

    }

    bool publish(const char* message) {
        LOG_IN("Publishing message: %s", message);
        if(!_connected) {
             LOG_RET_FALSE("Failed to send heartbeat. AMQP conn disconnected");
         }
        amqp_bytes_t body_bytes = amqp_cstring_bytes(message);
        const char* context = "Publishing Message";
        LOG_TRACE("Publishing exchange: %s, routing_key:%s", _exchange.c_str(), _routing_key.c_str());
        int status = amqp_basic_publish(_conn, _channel, amqp_cstring_bytes(_exchange.c_str()),
                amqp_cstring_bytes(_routing_key.c_str()), 0, 0, NULL, body_bytes);

        LOG_TRACE("publish status: %d", status);
        if(log_error(status, context)) {
            LOG_RET_TRUE("Published successfully");
        }
        _connected = false;
        LOG_RET_FALSE("Published failed");
    }

    bool send_heartbeat() {
         LOG_IN("");
         if(!_connected) {
             LOG_RET_FALSE("Failed to send heartbeat. AMQP conn disconnected");
         }
        const char* context = "Sending heartbeat";
        amqp_frame_t hb;
        hb.channel = 0;
        hb.frame_type = AMQP_FRAME_HEARTBEAT;

        int status = amqp_send_frame(_conn, &hb);
        if(log_error(status, context)) {
            LOG_RET_TRUE("Heartbeat sent successfully");
        }
        _connected = false;
        LOG_RET_FALSE("Failed to send heartbeat");
        
    }
    
    bool is_connected() {
        return _connected;
    }

    virtual ~amqp_connection() {
         LOG_IN("");
            //LOG_DEBUG("Closing AMQP Channel");
        if (_conn && _connected) {
            amqp_channel_close(_conn, 1, AMQP_REPLY_SUCCESS);
            //LOG_DEBUG("Closing AMQP Connection");
            amqp_connection_close(_conn, AMQP_REPLY_SUCCESS);
            //LOG_DEBUG("Destroying AMQP Connection");
            amqp_destroy_connection(_conn);
        }
         LOG_OUT("")
    }
private:

    bool log_amqp_error(amqp_rpc_reply_t x, const char *context) {
         LOG_IN("");
        char error[256];
        switch (x.reply_type) {
            case AMQP_RESPONSE_NORMAL:
                LOG_DEBUG("AMQP_RESPONSE_NORMAL");
                LOG_RET_TRUE("AMQP_RESPONSE_NORMAL: success");

            case AMQP_RESPONSE_NONE:
                LOG_INFO("AMQP_RESPONSE_NONE: %s: missing RPC reply type!\n", context);
                break;

            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                LOG_ERROR("AMQP_RESPONSE_LIBRARY_EXCEPTION: %s: %s\n", context, amqp_error_string2(x.library_error));
                break;

            case AMQP_RESPONSE_SERVER_EXCEPTION:
                switch (x.reply.id) {
                    case AMQP_CONNECTION_CLOSE_METHOD:
                    {
                        amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
                        LOG_ERROR("AMQP_CONNECTION_CLOSE_METHOD: %s: server connection error %d, message: %.*s\n",
                                context,
                                m->reply_code,
                                (int) m->reply_text.len, (char *) m->reply_text.bytes);
                        
                        break;
                    }
                    case AMQP_CHANNEL_CLOSE_METHOD:
                    {
                        amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
                        LOG_ERROR("AMQP_CHANNEL_CLOSE_METHOD: %s: server channel error %d, message: %.*s\n",
                                context,
                                m->reply_code,
                                (int) m->reply_text.len, (char *) m->reply_text.bytes);
                        break;
                    }
                    default:
                        LOG_ERROR("Default: %s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                        break;
                }
                break;
        }

       LOG_RET_FALSE("");
    }

    bool log_error(int x, const char* context) {
        if (x < 0) {
            LOG_ERROR("%s: %s", context, amqp_error_string2(x));
            LOG_RET_FALSE("returning false");
        }
        LOG_RET_TRUE("success");
    }


    amqp_connection_state_t _conn;
    bool _connected;
    amqp_basic_properties_t _props;
    //amqp_socket_t *_socket;
    struct amqp_connection_info _ci;
    unsigned _channel_max;
    unsigned _frame_max;
    unsigned _heartbeat;
    amqp_sasl_method_enum _method;
    amqp_channel_t _channel;
    std::string _exchange;
    std::string _uri;
    std::string _routing_key;
    


};

#endif /* AMQP_CONNECTION_H */

