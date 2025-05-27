package com.udb.server.controller;
/**
 * This class is used to return the result of the request.
 * It contains the status, message, and data.
 * @author Udb
 */
public class Result {
    private String status;
    private String message;
    private Object data;
    public Result() {
        
    }
    public Result(String status, String message, Object data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }
    public static Result success(Object data) {
        return new Result("success", "success", data);
    }
    public static Result fail(String message) {
        return new Result("fail", message, null);
    }

    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public Object getData() {
        return data;
    }
    public void setData(Object data) {
        this.data = data;
    }
    
}
