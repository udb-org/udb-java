package com.udb.model;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.service.BaseService;
import com.zaxxer.hikari.HikariDataSource;
/**
 * The MysqlToJsonExporter class is used to export data from MySQL to JSON.
 * It uses the FastJSON library to export data.
 * @author Udb
 * @version 1.0
 * @since 1.0
 * 
 */
public class MysqlToJsonExporter {
    public static void exportToJson(String args, String path) {
        System.out.println("Exporting data to JSON file: " + args);
        try {
            JSONArray argsJson = JSONArray.parseArray(args);
            for (int d = 0; d < argsJson.size(); d++) {
                String databaseName = argsJson.getJSONObject(d).getString("database");
                HikariDataSource dataSource = BaseService.getDataSource(databaseName);
                if (dataSource == null) continue;
                System.out.println("Exporting database: " + databaseName);

                JSONArray tablesJson = argsJson.getJSONObject(d).getJSONArray("tables");
                try (Connection conn = dataSource.getConnection();
                     Statement stmt = conn.createStatement(
                         ResultSet.TYPE_FORWARD_ONLY,
                         ResultSet.CONCUR_READ_ONLY)) {
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    for (int j = 0; j < tablesJson.size(); j++) {
                        String tableName = tablesJson.getString(j);           
                        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                            ResultSetMetaData metaData = rs.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            
                            JSONArray tableArray = new JSONArray();
                            while (rs.next()) {
                                JSONObject jsonObj = new JSONObject();
                                for (int i = 1; i <= columnCount; i++) {
                                    String colName = metaData.getColumnName(i);
                                    jsonObj.put(colName, rs.getObject(i));
                                }
                                tableArray.add(jsonObj);
                            }
                            try (FileWriter writer = new FileWriter(path+"/"+databaseName+"."+tableName+".json")) {
                                writer.write(tableArray.toJSONString());
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("JSON export failed", e);
        }
    }
}
