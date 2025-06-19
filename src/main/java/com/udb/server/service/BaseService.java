package com.udb.server.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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

        System.out.println("initDataSource");
        System.out.println(datasourceJson.toJSONString());

        String key = datasourceJson.getString("name") + ":" + datasourceJson.getString("database");
        String username = datasourceJson.getString("username");
        String password = datasourceJson.getString("password");
        String jdbcUrl = datasourceJson.getString("driverJdbcUrl");
        String driverMainClass = datasourceJson.getString("driverMainClass");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverMainClass);
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

}
