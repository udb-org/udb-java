package com.udb.model;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.excel.write.metadata.WriteTable;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.udb.server.service.BaseService;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
/**
 * The MysqlToXlsxExporter class is used to export data from MySQL to XLSX.
 * It uses the EasyExcel library to export data.
 * @author Udb
 * @version 1.0
 * @since 1.0
 */
public class MysqlToXlsxExporter {
    public static void exportToXlsx(String data, String path) {
        JSONArray argsJson = JSONArray.parseArray(data);
        System.out.println("Starting to export data to XLSX..."+data);
        for (int d = 0; d < argsJson.size(); d++) {
            String databaseName = argsJson.getJSONObject(d).getString("database");
            HikariDataSource dataSource = BaseService.getDataSource(databaseName);
            if (dataSource == null)
                continue;
            try (ExcelWriter excelWriter = EasyExcel.write(path + "/" + databaseName + ".xlsx").build()) {
                JSONArray tables = argsJson.getJSONObject(d).getJSONArray("tables");
                for (int j = 0; j < tables.size(); j++) {
                    String tableName = tables.getString(j);
                    try (Connection conn = dataSource.getConnection();
                            Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                            System.out.println("Exporting table " + tableName + " to XLSX...");
                            System.out.println(rs.getMetaData().getColumnCount());
                            
                            ResultSetMetaData metaData = rs.getMetaData();
                          
                            //Create a sheet and a table
                            WriteSheet writeSheet = EasyExcel.writerSheet(j, tableName).build();
                            WriteTable writeTable = new WriteTable();
                            //Set the table header
                            List<String> headers = new ArrayList<>();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                headers.add(metaData.getColumnName(i));
                            }
                            System.out.println(JSON.toJSONString(headers));
                 
                            //Write the data
                            List<List<Object>> dataList = new ArrayList<>();
                            while (rs.next()) {
                                List<Object> rowData = new ArrayList<>();
                                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                    rowData.add(rs.getObject(i));
                                }
                                dataList.add(rowData);
                            }
                            System.out.println("Data rows: " + dataList.size());
                           
                            writeSheet.setHead(headers.stream().map(List::of).toList());
                            excelWriter.write(dataList, writeSheet);
                            excelWriter.finish();
                            
                        }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to export data to XLSX", e);
            }
        }

    }
}
