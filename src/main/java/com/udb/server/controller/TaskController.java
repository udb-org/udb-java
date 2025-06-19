package com.udb.server.controller;

import java.util.Map;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.udb.server.bodies.Result;
import com.udb.server.bodies.TaskBody;
import com.udb.server.service.TaskService;

/**
 * The BaseController class is the controller of the application.
 * It provides methods for data source management, SQL execution, and task
 * management.
 * 1. Supports multiple SQL statements separated by semicolons.
 * 2. Asynchronous execution.
 * 3. Asynchronous results.
 * 4. Supports transactions.
 * 
 * @author Udb
 * @version 1.0
 * @since 1.0
 */
@RestController
@RequestMapping("/api/task")
public class TaskController {

    /**
     *
     * Start executing SQL statements
     * 
     * @param body
     * @return sessionId
     */
    @RequestMapping("/run")
    @ResponseBody
    public Result run(@RequestBody Map<String, Object> body) {
        return TaskService.run(body);
    }

    /**
     *
     * Get the result of a task
     * 
     */
    @RequestMapping("/result")
    @ResponseBody
    public Result result(@RequestBody TaskBody body) {
        return TaskService.result(body);

    }

    /**
     * Stop a task
     */
    @RequestMapping("/stop")
    @ResponseBody
    public Result stop(@RequestBody TaskBody body) {
        return TaskService.stop(body);
    }

    /**
     * Commit a transaction
     * 
     * @param id
     * @return
     */
    @RequestMapping("/commit")
    @ResponseBody
    public Result commit(@RequestBody TaskBody body) {
        return TaskService.commit(body);
    }

    /**
     * Rollback a transaction
     * 
     * @param id
     * @return
     */
    @RequestMapping("/rollback")
    @ResponseBody
    public Result rollback(@RequestBody TaskBody body) {
        return TaskService.rollback(body);
    }

    /**
     * Get all tasks
     */
    @RequestMapping("/list")
    @ResponseBody
    public Result list() {
        return TaskService.list();
    }

}
