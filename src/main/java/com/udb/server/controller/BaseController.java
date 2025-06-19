package com.udb.server.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson2.JSON;
import com.udb.model.MysqlToCsvExporter;
import com.udb.model.MysqlToJsonExporter;
import com.udb.model.MysqlToSqlExporter;
import com.udb.model.MysqlToXlsxExporter;
import com.udb.server.bodies.ExeSqlBody;
import com.udb.server.bodies.ExportDataBody;
import com.udb.server.bodies.Result;
import com.udb.server.service.BaseService;

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
@RequestMapping("/api/base")
public class BaseController {
    @RequestMapping("/getDataSources")
    @ResponseBody
    public Result getDataSources() {
        try {
            return Result.success(BaseService.getDataSources());
        } catch (Exception e) {
            // Get data source failed
            return Result.error(e.getMessage());
        }
    }

    /**
     * Execute SQL statements，synchronously
     * 
     * @param body
     * @return
     */
    @RequestMapping("/executeSql")
    @ResponseBody
    public Result execSql(@RequestBody ExeSqlBody body) {
        System.out.println(body.getSql());
        try {
            return BaseService.executeSql(body);
        } catch (Exception e) {
            return Result.error(e.getMessage());
        }
    }

    @RequestMapping("/exportData")
    @ResponseBody
    public Result exportData(@RequestBody ExportDataBody body) {
        System.out.println(JSON.toJSONString(body));
        try {
            if (body.getFormat().equals("csv")) {
                MysqlToCsvExporter.exportToCsv(body.getData(), body.getPath());
            } else if (body.getFormat().equals("json")) {
                MysqlToJsonExporter.exportToJson(body.getData(), body.getPath());
            } else if (body.getFormat().equals("excel")) {
                MysqlToXlsxExporter.exportToXlsx(body.getData(), body.getPath());
            } else if (body.getFormat().equals("sql")) {
                MysqlToSqlExporter.exportToSql(body.getData(), body.getPath());
            }
            return Result.success("success");
        } catch (Exception e) {
            return Result.error(e.getMessage());
        }
    }
}
