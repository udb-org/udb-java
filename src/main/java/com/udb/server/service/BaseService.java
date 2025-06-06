package com.udb.server.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.bodies.ExeSqlBody;
import com.udb.server.bodies.Result;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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
public class BaseService {
    private static Map<String, HikariDataSource> dataSourceMap;

    public static HikariDataSource getDataSource(String dataSourceName) {
        return BaseService.dataSourceMap.get(dataSourceName);
    }

    public static HikariDataSource initDataSource(JSONObject datasourceJson) {
        String key = datasourceJson.getString("name") + ":" + datasourceJson.getString("database");
        String type = datasourceJson.getString("type");
        String driver = datasourceJson.getString("driver");
        String host = datasourceJson.getString("host");
        int port = datasourceJson.getIntValue("port");
        String username = datasourceJson.getString("username");
        String password = datasourceJson.getString("password");
        String database = datasourceJson.getString("database");
        String params = datasourceJson.getString("params");

        if (type.equals("mysql")) {
            if (driver == null || driver.length() < 10) {
                driver = ("com.mysql.cj.jdbc.Driver");
            }
            if (database == null || database.length() == 0) {
                database = ("mysql");
            }
        } else if (type.equals("oracle")) {
            if (driver == null || driver.length() < 10) {
                driver = ("oracle.jdbc.OracleDriver");
            }
            if (database == null || database.length() == 0) {
                database = ("orcl");
            }
        } else if (type.equals("sqlserver")) {
            if (driver == null || driver.length() < 10) {
                driver = ("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            }
            if (database == null || database.length() == 0) {
                database = ("master");
            }
        } else if (type.equals("postgresql")) {
            if (driver == null || driver.length() < 10) {
                driver = ("org.postgresql.Driver");
            }
            if (database == null || database.length() == 0) {
                database = ("postgres");
            }
        }

        StringBuffer url = new StringBuffer("jdbc:");
        url.append(type);
        url.append("://");
        url.append(host).append(":");
        url.append(port).append("/");
        url.append(database);

        String urlStr = url.toString();
        if (params != null && params.length() > 0) {
            urlStr = urlStr + "?" + params;
        }
        System.out.println(urlStr);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(urlStr);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driver);
        // set the connection to expire after 1000 minutes, and clean up expired
        // connections
        config.setMaxLifetime(1000 * 60);
        config.setIdleTimeout(1000 * 60);
        // set the minimum number of connections to 2
        config.setMaximumPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName(key);
        HikariDataSource ds = new com.zaxxer.hikari.HikariDataSource(config);
        dataSourceMap = new java.util.HashMap<>();
        dataSourceMap.put(key, ds);
        return ds;
    }

    /**
     * This method returns a list of data sources.
     * 
     * @return
     */
    public static List<Object> getDataSources() {
        List<Object> dataSources = new java.util.ArrayList<>();
        for (Map.Entry<String, HikariDataSource> entry : BaseService.dataSourceMap.entrySet()) {
            Map<String, Object> dataSource = new java.util.HashMap<>();
            dataSource.put("name", entry.getKey());
            dataSource.put("url", entry.getValue().getJdbcUrl());
            dataSource.put("username", entry.getValue().getUsername());
            dataSource.put("password", entry.getValue().getPassword());
            dataSource.put("driverClassName", entry.getValue().getDriverClassName());
            dataSources.add(dataSource);
        }
        return dataSources;
    }

    /**
     * This method returns a data source.
     * 
     * @param datasourceJson
     * @return
     */
    public static HikariDataSource getDataSource(JSONObject datasourceJson) {
        String key = datasourceJson.getString("name") + ":" + datasourceJson.getString("database");
        if (BaseService.dataSourceMap == null) {
            return initDataSource(datasourceJson);
        }
        HikariDataSource dataSource = BaseService.dataSourceMap.get(key);
        if (dataSource != null) {
            return dataSource;
        } else {
            return initDataSource(datasourceJson);
        }
    }

    // The map stores the execution tasks
    private static Map<String, ExecThread> threadPoolTaskMap = new java.util.HashMap<>();

    /**
     * This method executes SQL statements.
     * It returns a JSON object that contains the execution status, start time, end
     * time, and results.
     * 
     * @param body
     * @return id
     */

    public static Result exec(ExeSqlBody body) {
        // If the number of tasks is greater than 10, return an error message
        if (threadPoolTaskMap.size() >= 10) {
            return new Result(100).message("Too many tasks");
        }
        String id = UUID.randomUUID().toString();
        ExecThread thread = new ExecThread(id, body.getSql(), body.getDatasource(), body.isTransaction());
        threadPoolTaskMap.put(id, thread);
        thread.start();
        return Result.running().id(id).startTime(thread.getStartTime()).endTime(thread.getEndTime());
    }

    /**
     * This method returns a list of tasks.
     * 
     * @return
     */
    public static Result getTasks() {
        JSONArray tasks = new JSONArray();
        for (Map.Entry<String, ExecThread> entry : threadPoolTaskMap.entrySet()) {
            JSONObject task = new JSONObject();
            task.put("id", entry.getKey());
            task.put("startTime", entry.getValue().getStartTime());
            task.put("status", entry.getValue().getStatus());
            task.put("endTime", entry.getValue().getEndTime());
            task.put("errorMessage", entry.getValue().getErrorMessage());
            task.put("lable", entry.getValue().getLable());
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
    public static Result getResult(String id) {
        ExecThread thread = threadPoolTaskMap.get(id);
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
                    .id(id);
            if (thread.getStatus() != 200) {
                rs.setStatus(thread.getStatus());
                rs.setMessage(thread.getErrorMessage());
            }
            if (!thread.isTransaction() || thread.isCommitOrRollback()) {
                // Close the connection
                thread.end();
                threadPoolTaskMap.remove(id);
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
    public static Result stop(String id) {
        ExecThread thread = threadPoolTaskMap.get(id);
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
    public static Result commit(String id) {
        ExecThread thread = threadPoolTaskMap.get(id);
        if (thread == null) {

            return new Result(820).message("Task does not exist");
        }
        try {
            thread.commit();
            thread.end();
            threadPoolTaskMap.remove(id);

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
    public static Result rollback(String id) {
        ExecThread thread = threadPoolTaskMap.get(id);
        if (thread == null) {

            return new Result(820).message("Task does not exist");
        }
        try {
            thread.rollback();
            thread.end();
            threadPoolTaskMap.remove(id);

            return Result.success().id(id).message("Rollback success");
        } catch (Exception e) {

            return Result.error(e.getMessage()).id(id);
        }

    }

    /**
     * This method executes SQL statements.
     * It returns a JSON object that contains the execution status, start time, end
     * time, and results.
     * 
     * @param body
     * @return
     * @throws Exception
     */
    public static Result executeSql(ExeSqlBody body) throws Exception {
        JSONObject datasourceJson = JSONObject.parseObject(body.getDatasource());
        HikariDataSource dataSource = getDataSource(datasourceJson);
        if (dataSource == null) {
            return new Result(830).message("Data source does not exist");
        }
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            java.sql.Statement stmt = conn.createStatement();
            boolean isResult = stmt.execute(body.getSql());
            java.util.List<Map<String, Object>> columns = new java.util.ArrayList<>();
            java.util.List<Map<String, Object>> rows = new java.util.ArrayList<>();
            if (isResult) {
                java.sql.ResultSet rs = stmt.getResultSet();
                // Get the column information
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    Map<String, Object> column = new java.util.HashMap<>();
                    column.put("columnLable", rs.getMetaData().getColumnLabel(i));
                    column.put("columnTypeName", rs.getMetaData().getColumnTypeName(i));
                    column.put("columnName", rs.getMetaData().getColumnName(i));
                    column.put("columnDisplaySize", rs.getMetaData().getColumnDisplaySize(i));
                    column.put("columnType", rs.getMetaData().getColumnType(i));
                    columns.add(column);
                }
                // Get the data
                while (rs.next()) {
                    Map<String, Object> row = new java.util.HashMap<>();
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
                rs.close();
            } else {
                long updateCount = stmt.getLargeUpdateCount();
                Map<String, Object> row = new java.util.HashMap<>();
                row.put("updateCount", updateCount);
                rows.add(row);
            }
            stmt.close();
            conn.close();
            Map<String, Object> result = new java.util.HashMap<>();
            result.put("columns", columns);
            result.put("rows", rows);
            return Result.success(result);
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
            return Result.error(e.getMessage());
        }

    }

    private static Map<String, DumpThread> dumpThreadMap = new java.util.HashMap<>();

    public static Result dumpDatabase(String datasource, String path, String tables, String dumpType) throws Exception {
        String key = UUID.randomUUID().toString();
        if (dumpThreadMap.containsKey(key)) {
            return new Result(840).message("Dump is running");
        }
        DumpThread thread = new DumpThread(key, tables, dumpType, path, datasource);
        dumpThreadMap.put(key, thread);
        thread.start();
        return Result.running().id(key).startTime(thread.getStartTime()).message("Dumping");
    }

    public static Result getDumpResult(String id) {
        DumpThread thread = dumpThreadMap.get(id);
        if (thread == null) {

            return new Result(820).message("Task does not exist");
        }
        // Add the results
        String results = "[";
        if (thread.getResults() != null && thread.getResults().size() > 0) {
            while (thread.getResults().size() > 0) {
                String result = thread.getResults().poll();
                results += "\"" + result + "\",";
            }
        }
        results += "]";

        // Add the error message
        if (thread.getEndTime() == null) {
            return Result.running().id(id).startTime(thread.getStartTime()).endTime(thread.getEndTime());
        } else {
            Result rs = Result.success().data(results).startTime(thread.getStartTime()).endTime(thread.getEndTime())
                    .id(id);
            if (thread.getStatus() != 200) {
                rs.setStatus(thread.getStatus());
            }
            thread.end();
            dumpThreadMap.remove(id);
            return rs;
        }
    }

    public static Result stopDump(String id) {
        DumpThread thread = dumpThreadMap.get(id);
        if (thread == null) {
            return new Result(820).message("Task does not exist");
        }
        return thread.end();
    }
}
