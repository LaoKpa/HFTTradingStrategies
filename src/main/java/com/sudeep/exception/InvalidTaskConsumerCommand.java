package com.sudeep.exception;

public class InvalidTaskConsumerCommand extends RuntimeException{
    public InvalidTaskConsumerCommand(String msg){
        super(msg);
    }
}
