package com.udb.server.service.thread;

import java.sql.SQLException;
import java.util.Map;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.service.BaseService;
import com.udb.server.service.ComThread;
import com.zaxxer.hikari.HikariDataSource;

/**
 * This class extends the Thread class and is used to execute SQL statements in
 * a separate thread.
 * It provides methods for managing the execution of SQL statements, including
 * starting, ending, and committing/rolling back transactions.
 * It also provides methods for managing the execution of SQL statements in a
 * transaction.
 * It uses HikariCP for connection pooling and FastJSON2 for JSON processing.
 */
public class SQLThread extends ComThread {
    private String sql;
    private String datasource;

    public SQLThread(String sessionId, Map<String, Object> body) {
        this.sessionId = sessionId;
        this.sql = body.get("sql").toString();
        this.datasource = body.get("datasource").toString();
        this.isTransaction = body.get("transaction").toString().equals("true");
    }

    public String getType() {
        return "sql";
    }

    public String getLable() {
        if (sql.length() > 30) {
            return sql.substring(0, 30) + "...";
        }
        return sql;
    }

    @Override
    public void run() {
        try {
            startTime = new java.util.Date();
            System.out.println("Start executing");
            // 创建数据源
            JSONObject datasourceJson = JSONObject.parseObject(datasource);
            HikariDataSource dataSource = BaseService.getDataSource(datasourceJson);
            if (dataSource == null) {
                System.out.println("datasource does not exist");
                message = "datasource does not exist";
                endTime = new java.util.Date();
                status = 830;
                return;

            }
            System.out.println("datasource exists");
            String[] sqls = sql.split(";");
            // Create a connection
            conn = dataSource.getConnection();
            if (isTransaction) {
                conn.setAutoCommit(false);
            }
            // Execute query statements
            if (sqls.length > 0) {
                if (results == null) {
                    results = new java.util.concurrent.ArrayBlockingQueue<>(sqls.length);
                }
                for (int i = 0; i < sqls.length; i++) {
                    String sql = sqls[i];
                    query(sql, i);
                    progress = Math.round(i * 10000 / sqls.length) * 100.0;
                    message = "Execute sql:" + sql;
                }
            }
            endTime = new java.util.Date();
            System.out.println("Execute success");
            message = "Execute success";
            status = 200;
            
        } catch (Exception e) {
            try {
                if (conn != null) {
                    if (isTransaction) {
                        conn.rollback();
                    }
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            message = e.getMessage();
            endTime = new java.util.Date();
            status = 500;
        }
    }

    /**
     * Execute query statements.
     *
     * @param sql
     * @param index
     * @throws Exception
     */
    private void query(String sql, long index) throws Exception {
        try {
            System.out.println("Execute sql:" + sql);
            java.sql.Statement stmt = conn.createStatement();
            boolean isResult = stmt.execute(sql);
            java.util.List<Map<String, Object>> columns = new java.util.ArrayList<>();
            java.util.List<Map<String, Object>> rows = new java.util.ArrayList<>();
            if (isResult) {
                java.sql.ResultSet rs = stmt.getResultSet();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    Map<String, Object> column = new java.util.HashMap<>();
                    column.put("columnLable", rs.getMetaData().getColumnLabel(i));
                    column.put("columnTypeName", rs.getMetaData().getColumnTypeName(i));
                    column.put("columnName", rs.getMetaData().getColumnName(i));
                    column.put("columnDisplaySize", rs.getMetaData().getColumnDisplaySize(i));
                    column.put("columnType", rs.getMetaData().getColumnType(i));
                    columns.add(column);
                }
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
                Map<String, Object> column = new java.util.HashMap<>();
                column.put("columnLable", "updateCount");
                column.put("columnTypeName", "updateCount");
                column.put("columnName", "updateCount");
                column.put("columnDisplaySize", 10);
                column.put("columnType", 10);
                columns.add(column);
            }
            stmt.close();
            Map<String, Object> result = new java.util.HashMap<>();
            result.put("columns", columns);
            result.put("rows", rows);
            result.put("index", index);
            result.put("sql", sql);
            result.put("status", "success");
            result.put("message", "Execute success");
            System.out.println(JSON.toJSONString(result));
            results.put(result);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            Map<String, Object> result = new java.util.HashMap<>();
            result.put("index", index);
            result.put("sql", sql);
            result.put("status", "fail");
            result.put("message", e.getMessage());
            results.put(result);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}
