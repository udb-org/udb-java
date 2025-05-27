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
 * The MysqlToCsvExporter class is used to export data from MySQL to CSV files.
 * @author Udb
 * @version 1.0
 * @since 1.0
 * 
 */
public class MysqlToCsvExporter {
    /**
     * Export data from MySQL to CSV files.
     * @param args
     * @param path
     * @throws IOException
     */
    public static void exportToCsv(String args,String path) throws IOException {
       System.out.println("Export data to CSV file:"+args);
        JSONArray argsJson = JSONArray.parseArray(args);
     
      
        for (int d = 0; d < argsJson.size(); d++) {
            String databaseName = argsJson.getJSONObject(d).getString("database");
            HikariDataSource dataSource = BaseService.getDataSource(databaseName);
            if (dataSource == null) {
                continue;
            }
            System.out.println("Export database:"+databaseName);
            JSONArray tablesJson = argsJson.getJSONObject(d).getJSONArray("tables");
            for (int j = 0; j < tablesJson.size(); j++) {
                String tableName = tablesJson.getString(j);
                System.out.println("Export table:"+tableName);
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                BufferedWriter writer = null;
                String csvFilePath = path+"/"+databaseName+"."+tableName+".csv";
                try {
                    conn = dataSource.getConnection();
                    stmt = conn.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY, 
                        ResultSet.CONCUR_READ_ONLY
                    );
                    stmt.setFetchSize(Integer.MIN_VALUE);  
                    rs = stmt.executeQuery("select * from " + tableName);
                    writer = new BufferedWriter(new FileWriter(csvFilePath));
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    StringBuilder header = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        header.append(escapeCsv(metaData.getColumnName(i)));
                        if (i < columnCount) header.append(",");
                    }
                    writer.write(header.toString());
                    writer.newLine();
                    while (rs.next()) {
                        StringBuilder line = new StringBuilder();
                        for (int i = 1; i <= columnCount; i++) {
                            String value = rs.getString(i);
                            line.append(escapeCsv(value));
                            if (i < columnCount) line.append(",");
                        }
                        writer.write(line.toString());
                        writer.newLine();
                    }
                    System.out.println("Export table " + tableName + " to CSV file successfully");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 关闭资源
                    try { if (writer != null) writer.close(); } catch (Exception e) {}
                    try { if (rs != null) rs.close(); } catch (Exception e) {}
                    try { if (stmt != null) stmt.close(); } catch (Exception e) {}
                    try { if (conn != null) conn.close(); } catch (Exception e) {}
                }             
            }
        }      
    }
    private static String escapeCsv(String value) {
        if (value == null) return "";
        String escaped = value.replace("\"", "\"\"");
        if (escaped.contains(",") || escaped.contains("\n") || escaped.contains("\"")) {
            return "\"" + escaped + "\"";
        }
        return escaped;
    }

 

}
