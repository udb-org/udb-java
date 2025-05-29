package com.udb.server.service;

import java.io.FileWriter;
import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.bodies.Result;
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
public class DumpThread extends Thread {
    private String sessionId;
    private String tables;
    private String dumpType;
    private String path;
    private String datasource;

    public DumpThread(String sessionId, String tables, String dumpType, String path, String datasource) {

        System.out.println("DumpThread created");
        System.out.println("sessionId: " + sessionId);
        System.out.println("tables: " + tables);
        System.out.println("dumpType: " + dumpType);
        System.out.println("path: " + path);
        System.out.println("datasource: " + datasource);

        this.sessionId = sessionId;
        this.tables = tables;
        this.dumpType = dumpType;
        this.path = path;
        this.datasource = datasource;

    }

    public String getSessionId() {
        return sessionId;
    }

    public String getDatasource() {
        return datasource;
    }

    private Connection conn;
    private Statement stmt;

    public Connection getConn() {
        return conn;
    }

    public Statement getStmt() {
        return stmt;
    }

    private BlockingQueue<String> results;

    /**
     * Get the results.
     * 
     * @return
     */
    public BlockingQueue<String> getResults() {
        return results;
    }

    private boolean isSuccess;

    /**
     * Is it successful?
     * 
     * @return
     */
    public boolean isSuccess() {
        return isSuccess;
    }

    private String errorMessage;

    /**
     * Get the error message.
     * 
     * @return
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    public Date startTime;
    public Date endTime;

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public Result end() {
        // Forcefully end the connection and close the resources
        try {
            if (conn != null) {
                conn.close();
            }
            this.interrupt();
            return new Result().success("Task has been terminated");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return new Result().fail(e.getMessage());
        }
    }

    @Override
    public void run() {
        FileWriter fileWriter = null;
        try {
            startTime = new java.util.Date();
            System.out.println("Start executing");
            // 创建数据源
            JSONObject datasourceJson = JSONObject.parseObject(datasource);
            HikariDataSource dataSource = BaseService.getDataSource(datasourceJson);
            if (dataSource == null) {
                System.out.println("datasource does not exist");
                errorMessage = "datasource does not exist";
                isSuccess = false;
                endTime = new java.util.Date();
                return;

            }
            System.out.println("datasource exists");
            this.conn = dataSource.getConnection();
            if (results == null) {
                results = new java.util.concurrent.ArrayBlockingQueue<>(100);
            }
            // file
            fileWriter = new FileWriter(path);
            JSONArray tablesArray = JSONArray.parseArray(tables);
            for (int i = 0; i < tablesArray.size(); i++) {
                String table = tablesArray.getString(i);
                results.add(Math.round(i * 100 / tablesArray.size()) + "%:" + table);
                dumpTable(table, fileWriter);
            }
            fileWriter.close();
            endTime = new java.util.Date();
            System.out.println("Execute success");
            isSuccess = true;
        } catch (Exception e) {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            errorMessage = e.getMessage();
            endTime = new java.util.Date();
        }
    }

    private void dumpTable(String table, FileWriter fileWriter) throws Exception {
        fileWriter.write("--- Dump Table:" + table + "---\n");

        try {
            if (dumpType.equals("sd")) {
                dumpTableStructure(table, fileWriter);
                dumpTableData(table, fileWriter);
            } else if (dumpType.equals("s")) {
                dumpTableStructure(table, fileWriter);
            } else if (dumpType.equals("d")) {
                dumpTableData(table, fileWriter);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fileWriter.write("--- Error:" + table + "---\n");
            if (e.getMessage() != null) {
                fileWriter.write("---" + e.getMessage().replace("\n", " ") + "---\n");
            }
        }
    }

    private void dumpTableStructure(String table, FileWriter fileWriter) throws Exception {
        fileWriter.write("--- Dump Table Structure:" + table + "---\n");
        fileWriter.write("DROP TABLE IF EXISTS `" + table + "`;\n");
        // select ddl
        String sql = "SHOW CREATE TABLE  " + table;
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            fileWriter.write(rs.getString(2) + ";\n");
        }
        rs.close();
        stmt.close();
    }

    private void dumpTableData(String table, FileWriter fileWriter) throws Exception {
        fileWriter.write("--- Dump Table Data:" + table + "---\n");
        // 查询数据个数
        String sql = "SELECT COUNT(*) FROM  " + table;
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            long count = rs.getLong(1);
            fileWriter.write("--- Total:" + count + "---\n");
            rs.close();
            stmt.close();
            //
            long pageSize = 1000;
            if (count <= pageSize) {
                dumpTableDataPage(table, fileWriter, 0, count);
            } else {
                long pageCount = count / pageSize;
                for (int i = 0; i < pageCount; i++) {
                    dumpTableDataPage(table, fileWriter, i * pageSize, pageSize);
                }
            }
        }
    }

    private void dumpTableDataPage(String table, FileWriter fileWriter, long start, long length) throws Exception {
        fileWriter.write("--- Page:" + start + "," + length + "---\n");
        // 查询数据
        String sql = "SELECT * FROM  " + table + " LIMIT " + start + "," + length;
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            // insert into
            fileWriter.write("INSERT INTO `" + table + "` (");
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                fileWriter.write("`" + rs.getMetaData().getColumnName(i) + "`");
                if (i < rs.getMetaData().getColumnCount()) {
                    fileWriter.write(",");
                }
            }
            fileWriter.write(") VALUES (");
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if(rs.getString(i)==null){
                    fileWriter.write("NULL");
                }else{
                    fileWriter.write("'" + rs.getString(i).replace("'", "''") + "'");
                }
                  if (i < rs.getMetaData().getColumnCount()) {
                    fileWriter.write(",");
                }
            }
            fileWriter.write(");\n");

        }
        rs.close();
        stmt.close();
    }
}
