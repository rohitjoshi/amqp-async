

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include "../include/ffi.h"
#include "../include/client_instance.h"

/*
     * Initialize AMQP instance
     * @uri : AMQP uri
     * @exchange : AMQP Exchange name
     * @routing_key: AMQP routing key
     * @log_dir : output log dir
     * @log_prefix : log file name
     */
    bool init(const char* uri, const char* exchange, const char* routing_key, 
                const char* log_dir, const char* logfile_prefix, unsigned log_level, char* error_msg){
        if(!uri) {
            sprintf(error_msg,"Failed to initialize. uri is null.\n");
            return -1;
            
        }else if(!exchange ) {
            sprintf(error_msg,"Failed to initialize. exchange is null.\n");
            return -1;
            
        }else if(!routing_key) {
            sprintf(error_msg,"Failed to initialize. routing_key is null.\n");
            return -1;
            
        }else if(!log_dir ) {
            sprintf(error_msg,"Failed to initialize. log_dir is null.\n");
            return -1;
            
        }else if(!logfile_prefix) {
            sprintf(error_msg,"Failed to initialize. logfile_prefix is null.\n");
            return -1;
            
        }
        return client_instance::init(uri, exchange, routing_key, log_dir, logfile_prefix,  log_level);
    }
    
    /*
     * publish a message
     */
    bool publish(const char* msg){
        return client_instance::publish(msg);
    }
    
    bool stop() {
        return client_instance::stop();
    }