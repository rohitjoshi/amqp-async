/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ffi.h
 * Author: Rohit Joshi <rohit.c.joshi@gmail.com>
 *
 * Created on October 28, 2017, 2:58 PM
 */

#ifndef FFI_H
#define FFI_H



#ifdef __cplusplus
extern "C" {
#endif

    /*
     * Initialize AMQP instance
     * @uri : AMQP uri
     * @exchange : AMQP Exchange name
     * @routing_key: AMQP routing key
     * @log_dir : output log dir
     * @log_prefix : log file name
     */
    int init(const char* uri, const char* exchange, const char* routing_key, 
            const char* log_dir, const char* logfile_prefix, unsigned log_level, char* error_msg);
    
    /*
     * publish a message
     */
    int publish(const char* msg);
    
    
    /**
     * Stop the thread
     */
    void stop();


#ifdef __cplusplus
}
#endif

#endif /* FFI_H */

