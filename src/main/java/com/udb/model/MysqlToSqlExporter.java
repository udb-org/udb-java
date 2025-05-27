package com.udb.model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import com.alibaba.fastjson2.JSONArray;
import com.udb.server.service.BaseService;
import com.zaxxer.hikari.HikariDataSource;
/**
 * The MysqlToSqlExporter class is used to export data from MySQL to SQL files.
 * @author Udb
 * @version 1.0
 * @since 1.0
 */
public class MysqlToSqlExporter {

    public static void exportToSql(String args, String path) throws IOException {
        System.out.println("Export data to SQL file:" + args);
        JSONArray argsJson = JSONArray.parseArray(args);

        for (int d = 0; d < argsJson.size(); d++) {
            String databaseName = argsJson.getJSONObject(d).getString("database");
            HikariDataSource dataSource = BaseService.getDataSource(databaseName);
            if (dataSource == null) {
                continue;
            }
            System.out.println("Export database:" + databaseName);
            String sqlFilePath = path + "/" + databaseName + ".sql";
            BufferedWriter writer = new BufferedWriter(new FileWriter(sqlFilePath));
            JSONArray tablesJson = argsJson.getJSONObject(d).getJSONArray("tables");
            for (int j = 0; j < tablesJson.size(); j++) {
                String tableName = tablesJson.getString(j);
                System.out.println("Export table:" + tableName);
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = dataSource.getConnection();
                    stmt = conn.createStatement(
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    rs = stmt.executeQuery("SELECT * FROM " + tableName);
                    // 写入SQL文件头
                    writer.write("-- SQL Export File - Database: " + databaseName + ", Table: " + tableName);
                    writer.newLine();
                    writer.write("-- Created Date: " + new java.util.Date());
                    writer.newLine();
                    writer.newLine();
                    
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    //Create a table
                    while (rs.next()) {
                        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                        for (int i = 1; i <= columnCount; i++) {
                            String value = rs.getString(i);
                            if (value == null) {
                                sql.append("NULL");
                            } else {
                                sql.append("'").append(escapeSql(value)).append("'");
                            }
                            if (i < columnCount)
                                sql.append(", ");
                        }
                        sql.append(");");
                        writer.write(sql.toString());
                        writer.newLine();
                    }
                    System.out.println("Export table " + tableName + " to SQL file successfully");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                    try {
                        if (rs != null)
                            rs.close();
                    } catch (Exception e) {
                    }
                    try {
                        if (stmt != null)
                            stmt.close();
                    } catch (Exception e) {
                    }
                    try {
                        if (conn != null)
                            conn.close();
                    } catch (Exception e) {
                    }
                }
            }
            if (writer != null)
                writer.close();

        }
    }

    // SQL Special Character Escape
    private static String escapeSql(String value) {
        if (value == null)
            return "";
        return value.replace("'", "''");
    }
}
