package com.sudeep.exception;

public class InvalidProcessorTypeException extends RuntimeException{
    public InvalidProcessorTypeException(String msg){
        super(msg);
    }
}
