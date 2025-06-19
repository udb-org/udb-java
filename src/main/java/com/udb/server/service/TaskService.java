package com.udb.server.service;

import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.bodies.Result;
import com.udb.server.bodies.TaskBody;
import com.udb.server.service.thread.DumpThread;
import com.udb.server.service.thread.ImportThread;
import com.udb.server.service.thread.SQLThread;

/**
 * 
 * BaseService
 * This class serves as the base service for the UDB server.
 * It provides various methods for database connection management,
 * SQL execution, and task management.
 * 
 * It supports multiple database types such as MySQL, Oracle, SQL Server, and
 * PostgreSQL.
 * It uses HikariCP for connection pooling and FastJSON2 for JSON processing.
 *
 * 
 * @author udb
 * @version 1.0
 * 
 */
public class TaskService {

    // The map stores the execution tasks
    private static Map<String, ComThread> taskMap = new java.util.HashMap<>();

    /**
     * This method executes SQL statements.
     * It returns a JSON object that contains the execution status, start time, end
     * time, and results.
     * 
     * @param body
     * @return id
     */

    public static Result run(Map<String, Object> body) {
        // If the number of tasks is greater than 10, return an error message
        if (taskMap.size() >= 10) {
            return new Result(100).message("Too many tasks");
        }
        if (!body.containsKey("type")) {
            return new Result(500).message("type is required");
        }
        String type = body.get("type").toString();
        String id = UUID.randomUUID().toString();
        if (type.equals("sql")) {
            ComThread thread = new SQLThread(id, body);
            taskMap.put(id, thread);
            thread.start();
        } else if (type.equals("dump")) {
            ComThread thread = new DumpThread(id, body);
            taskMap.put(id, thread);
            thread.start();
        }else if (type.equals("import")) {
            ComThread thread = new ImportThread(id, body);
            taskMap.put(id, thread);
            thread.start();
        }  else {
            return new Result(500).message("type is invalid");
        }
        return Result.running().id(id);
    }

    /**
     * This method returns a list of tasks.
     * 
     * @return
     */
    public static Result list() {
        JSONArray tasks = new JSONArray();
        for (Map.Entry<String, ComThread> entry : taskMap.entrySet()) {
            JSONObject task = new JSONObject();
            task.put("id", entry.getKey());
            task.put("startTime", entry.getValue().getStartTime());
            task.put("status", entry.getValue().getStatus());
            task.put("endTime", entry.getValue().getEndTime());
            task.put("errorMessage", entry.getValue().getMessage());
            task.put("lable", entry.getValue().getLable());
            task.put("progress", entry.getValue().getProgress());
            tasks.add(task);
        }
        return Result.success(tasks);
    }

    /**
     * This method returns the result of a task.
     * It returns a JSON object that contains the execution status, start time, end
     * time, and results.
     * 
     * @param id
     * @return
     */
    public static Result result(TaskBody body) {
        String id = body.getId();
        ComThread thread = taskMap.get(id);
        if (thread == null) {
            return new Result(820).message("Task does not exist");
        }
        // Add the results
        String results = "[";
        if (thread.getResults() != null && thread.getResults().size() > 0) {
            while (thread.getResults().size() > 0) {
                Map<String, Object> result = thread.getResults().poll();
                results += JSON.toJSONString(result) + ",";
            }
        }
        if (results.length() > 1) {
            results = results.substring(0, results.length() - 1);
        }
        results += "]";

        // Add the error message
        if (thread.getEndTime() == null) {
            return Result.running().id(id).startTime(thread.getStartTime()).endTime(thread.getEndTime());
        } else {
            Result rs = Result.success().data(results).startTime(thread.getStartTime()).endTime(thread.getEndTime())
                    .id(id).progress(thread.getProgress()).message(thread.getMessage());
            if (thread.getStatus() != 200) {
                rs.setStatus(thread.getStatus());
                rs.setMessage(thread.getMessage());
            }
            if (!thread.isTransaction() || thread.isCommitOrRollback()) {
                // Close the connection
                thread.end();
                taskMap.remove(id);
            }
            return rs;
        }
    }

    /**
     * This method stops a task.
     * 
     * @param id
     * @return
     */
    public static Result stop(TaskBody body) {
        String id = body.getId();
        ComThread thread = taskMap.get(id);
        if (thread == null) {
            return new Result(820).message("Task does not exist");
        }
        return thread.end();
    }

    /**
     * This method commits a task.
     * 
     * @param id
     * @return
     */
    public static Result commit(TaskBody body) {
        String id = body.getId();
        ComThread thread = taskMap.get(id);
        if (thread == null) {
            return new Result(820).message("Task does not exist");
        }
        try {
            thread.commit();
            thread.end();
            taskMap.remove(id);
            return Result.success().id(id).message("Commit success");
        } catch (Exception e) {

            return Result.error(e.getMessage()).id(id);
        }

    }

    /**
     * This method rolls back a task.
     *
     * @param id
     * @return
     */
    public static Result rollback(TaskBody body) {
        String id = body.getId();
        ComThread thread = taskMap.get(id);
        if (thread == null) {

            return new Result(820).message("Task does not exist");
        }
        try {
            thread.rollback();
            thread.end();
            taskMap.remove(id);
            return Result.success().id(id).message("Rollback success");
        } catch (Exception e) {

            return Result.error(e.getMessage()).id(id);
        }

    }

}
