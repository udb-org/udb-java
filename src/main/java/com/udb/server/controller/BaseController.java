package com.udb.server.controller;


import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.udb.server.bodies.ExportDataBody;
import com.alibaba.fastjson2.JSON;
import com.udb.server.bodies.ExeSqlBody;
import com.udb.server.service.BaseService;
import com.udb.model.MysqlToCsvExporter;
import com.udb.model.MysqlToJsonExporter;
import com.udb.model.MysqlToSqlExporter;
import com.udb.model.MysqlToXlsxExporter;
/**
 * The BaseController class is the controller of the application.
 * It provides methods for data source management, SQL execution, and task management.
 * 1. Supports multiple SQL statements separated by semicolons.
 * 2. Asynchronous execution.
 * 3. Asynchronous results.
 * 4. Supports transactions.
 * @author Udb
 * @version 1.0
 * @since 1.0
 */
@RestController
@RequestMapping("/api/base")
public class BaseController {
    @RequestMapping("/getDataSources")
    @ResponseBody
    public Result getDataSources() {
        try {
            return Result.success(BaseService.getDataSources());
        } catch (Exception e) {
            // TODO: handle exception
        }
        return Result.fail("Get data source failed");
    }
    /**
     *
     * Start executing SQL statements
     * @param body
     * @return sessionId
     */
    @RequestMapping("/exec")
    @ResponseBody
    public Object exec(@RequestBody ExeSqlBody body) {
       return BaseService.exec(body);
    }
    /**
     *
     * Get the result of a task
     * 
     */
    @RequestMapping("/getResult")
    @ResponseBody
    public Object getResult(@RequestBody ExeSqlBody body) {
       return BaseService.getResult(body.getId());
       
    }
    /**
     * Stop a task
     */
    @RequestMapping("/stop")
    @ResponseBody
    public Object stop(@RequestBody ExeSqlBody body) {
       return BaseService.stop(body.getId());
       
    }
    /**
     * Commit a transaction
     * @param id
     * @return
     */
    @RequestMapping("/commit")
    @ResponseBody
    public Object commit(@RequestBody ExeSqlBody body) {
       return BaseService.commit(body.getId());
       
    }
    /**
     * Rollback a transaction
     * @param id
     * @return
     */
    @RequestMapping("/rollback")
    @ResponseBody
    public Object rollback(@RequestBody ExeSqlBody body) {
       return BaseService.rollback(body.getId());
    }
    /**
     * Get all tasks
     */
    @RequestMapping("/getTasks")
    @ResponseBody
    public Object getTasks() {
       return BaseService.getTasks();
    }

    /**
     * Execute SQL statements，synchronously
     * @param body
     * @return
     */
    @RequestMapping("/executeSql")
    @ResponseBody
    public Result execSql(@RequestBody ExeSqlBody body) {
       System.out.println(body.getSql());
       try {
        return Result.success(BaseService.executeSql(body));
       } catch (Exception e) {
        return Result.fail(e.getMessage());
       }
    }

    @RequestMapping("/exportData")
    @ResponseBody
    public Result dumpData(@RequestBody ExportDataBody body) {
       System.out.println(JSON.toJSONString(body));
       try {
        if(body.getFormat().equals("csv")){
            MysqlToCsvExporter.exportToCsv(body.getData(),body.getPath()    );
        }else if(body.getFormat().equals("json")){
            MysqlToJsonExporter.exportToJson(body.getData(),body.getPath()    );
        }else if(body.getFormat().equals("excel")){
            MysqlToXlsxExporter.exportToXlsx(body.getData(),body.getPath()    );
        }else if (body.getFormat().equals("sql")){
            MysqlToSqlExporter.exportToSql(body.getData(),body.getPath()    );
        }
        return Result.success("success");
       } catch (Exception e) {
        return Result.fail(e.getMessage());
       }
    }
}
