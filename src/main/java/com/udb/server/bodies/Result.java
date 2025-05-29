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
    private String status;
    private String message;
    private Object data;
    private Date startTime;
    private Date endTime;
    private String id;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    
    public Result() {
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
    public Result(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public  Result success(String message) {
        this.status = "success";
        this.message = message;
        return this;
    }
    public  Result data(Object data) {
        this.status = "success";
        this.data = data;
        return this;
    }
    public  Result fail(String message) {
        this.status = "fail";
        this.message = message;
        return this;
    }
    public  Result running(String message) {
        this.status = "running";
        this.message = message;
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
}
