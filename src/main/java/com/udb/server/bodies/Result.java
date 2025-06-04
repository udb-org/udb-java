package com.udb.server.bodies;

import java.util.Date;
/**
 * The Result class is used to return the result of a request.
 * 
 * status: success 、fail 、running
 * id: task id
 * data: task result
 * startTime: task start time
 * endTime: task end time
 * message: task message
 *
 * @author Udb
 * @version 1.0
 * @since 1.0
 * 
 */
public class Result {
    private int status;
    private Object data;
    private Date startTime;
    private Date endTime;
    private String id;
    private String message;

    public static Result success() {
        Result result = new Result(200);
        return result;
    }
    public static Result success(Object data) {
        Result result = new Result(200);
        result.data = data;
        return result;
    }
    public static Result error(String message) {
        Result result = new Result(500);
        result.message = message;
        return result;
    }
    public static Result running() {
        Result result = new Result(800);
        return result;
    }
  
    public Result(int status) {
        this.status = status;
        
    }
    public Result data(Object data) {
        this.data = data;
        return this;
    }
    public Result startTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }
    public Result endTime(Date endTime) {
        this.endTime = endTime;
        return this;
    }
    public Result id(String id) {
        this.id = id;
        return this;
    }
    public Result message(String message) {
        this.message = message;
        return this;
    }


    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    
    public Date getStartTime() {
        return startTime;
    }
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
    public Date getEndTime() {
        return endTime;
    }
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    public Object getData() {
        return data;
    }
    public void setData(Object data) {
        this.data = data;
    }
    

    public int getStatus() {
        return status;
    }
    public void setStatus(int status) {
        this.status = status;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
}
