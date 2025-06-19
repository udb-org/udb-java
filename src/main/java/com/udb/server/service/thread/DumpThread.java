package com.udb.server.service.thread;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.converters.Converter;
import com.alibaba.excel.metadata.GlobalConfiguration;
import com.alibaba.excel.metadata.data.WriteCellData;
import com.alibaba.excel.metadata.property.ExcelContentProperty;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.udb.server.service.BaseService;
import com.udb.server.service.ComThread;
import com.zaxxer.hikari.HikariDataSource;

/**
 * This class extends the Thread class and is used to execute database uniform
 * statements in a separate thread.
 */
public class DumpThread extends ComThread {
    // database
    private JSONObject datasourceJson;
    // database uniform
    private String dropTableSql;
    private String ddlSql;
    private String pageSql;
    private String fieldTypes;
    private String identifierQuoteSymbol;
    // common
    private String path;
    private String tables;
    private String fileType;
    private String fileName;
    // sql
    private String dumpType;

    public DumpThread(String sessionId, Map<String, Object> body) {
        System.out.println("DumpThread created");
        System.out.println("sessionId: " + sessionId);
        this.sessionId = sessionId;
        // database
        this.datasourceJson = JSONObject.parseObject(body.get("datasource").toString());
        // database uniform
        this.dropTableSql = body.get("dropTableSql").toString();
        this.ddlSql = body.get("ddlSql").toString();
        this.pageSql = body.get("pageSql").toString();
        this.fieldTypes = body.get("fieldTypes").toString();
        this.identifierQuoteSymbol = body.get("identifierQuoteSymbol").toString();
        // common
        this.path = body.get("path").toString();
        this.fileType = body.get("fileType").toString();
        this.fileName = body.get("fileName").toString();
        this.tables = body.get("tables").toString();
        // sql
        if (this.fileType.equals("sql")) {
            this.dumpType = body.get("dumpType").toString();
        } else if (this.fileType.equals("xlsx")) {

        } else if (this.fileType.equals("csv")) {

        } else if (this.fileType.equals("json")) {

        }

    }

    public String getType() {
        return "dump";
    }

    public String getLable() {
        return datasourceJson.getString("name");
    }

    @Override
    public void run() {
        FileWriter fileWriter = null;
        ExcelWriter excelWriter = null;
        try {
            startTime = new java.util.Date();
            System.out.println("Start executing");
            // 创建数据源
            HikariDataSource dataSource = BaseService.getDataSource(datasourceJson);
            if (dataSource == null) {
                System.out.println("datasource does not exist");
                message = "datasource does not exist";

                endTime = new java.util.Date();
                status = 830;
                return;

            }
            System.out.println("datasource exists");
            this.conn = dataSource.getConnection();
            if (results == null) {
                results = new java.util.concurrent.ArrayBlockingQueue<>(100);
            }
            // excel,sql可以多个表在同一个文件
            // csv,json只能处理一个表
            // file
            if (this.fileType.equals("sql")) {
                fileWriter = new FileWriter(this.path + "/" + this.fileName + ".sql", Charset.forName("UTF-8"));
            } else if (this.fileType.equals("xlsx")) {
                FileOutputStream out = new FileOutputStream(this.path + "/" + this.fileName + ".xlsx");
                excelWriter = EasyExcel.write(out)
                .registerConverter(new DateConverter())
                .registerConverter(new TimestampConverter())
                .registerConverter(new TimeConverter())
                
                .build();
            }
            JSONArray tablesArray = JSONArray.parseArray(this.tables);
            for (int i = 0; i < tablesArray.size(); i++) {
                String table = tablesArray.getString(i);
                progress = Math.round(i * 10000 / tablesArray.size()) * 100.0;
                message = "Dump table:" + table;
                dumpTable(table, fileWriter, excelWriter);
            }
            if (fileWriter != null) {
                fileWriter.close();
            }
            if (excelWriter != null) {
                excelWriter.finish();
                excelWriter.close();
            }

            endTime = new java.util.Date();
            System.out.println("Execute success");

            message = "Execute success";
            status = 200;
            endTime = new java.util.Date();
        } catch (Exception e) {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            if (excelWriter != null) {
                excelWriter.finish();
                excelWriter.close();
            }
            try {
                if (conn != null) {
                    conn.close();
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

    private void dumpTable(String table, FileWriter fileWriter, ExcelWriter excelWriter) throws Exception {
        if (this.fileType.equals("sql")) {
            fileWriter.write("--- Dump Table:" + table + "---\n");
            try {
                if (this.dumpType.equals("sd")) {
                    dumpTableStructure(table, fileWriter);
                    dumpTableData(table, fileWriter, null, null);
                } else if (this.dumpType.equals("s")) {
                    dumpTableStructure(table, fileWriter);
                } else if (this.dumpType.equals("d")) {
                    dumpTableData(table, fileWriter, null, null);
                }
            } catch (Exception e) {
                e.printStackTrace();
                fileWriter.write("--- Error:" + table + "---\n");
                if (e.getMessage() != null) {
                    fileWriter.write("---" + e.getMessage().replace("\n", " ") + "---\n");
                }
            }
        } else if (this.fileType.equals("xlsx")) {
            WriteSheet writeSheet = EasyExcel.writerSheet(table).build();
            dumpTableData(table, fileWriter, excelWriter, writeSheet);

        } else if (this.fileType.equals("csv")) {
            FileWriter csvWriter = null;
            try {
                JSONArray tablesArray = JSONArray.parseArray(this.tables);
                String p = this.path + "/" + this.fileName + ".csv";
                if (tablesArray.size() > 1) {
                    p = this.path + "/" + this.fileName + "_" + table + ".csv";
                }
                csvWriter = new FileWriter(p,
                        Charset.forName("UTF-8"));
                dumpTableData(table, csvWriter, excelWriter, null);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (csvWriter != null) {
                    csvWriter.flush();
                    csvWriter.close();
                }
            }

        } else if (this.fileType.equals("json")) {
            FileWriter jsonWriter = null;
            try {
                JSONArray tablesArray = JSONArray.parseArray(this.tables);
                String p = this.path + "/" + this.fileName + ".json";
                if (tablesArray.size() > 1) {
                    p = this.path + "/" + this.fileName + "_" + table + ".json";
                }
                jsonWriter = new FileWriter(p,
                        Charset.forName("UTF-8"));
                jsonWriter.write("[");
                dumpTableData(table, jsonWriter, excelWriter, null);
                jsonWriter.write("]");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (jsonWriter != null) {
                    jsonWriter.flush();
                    jsonWriter.close();
                }
            }

        }
    }

    private void dumpTableStructure(String table, FileWriter fileWriter) throws Exception {
        fileWriter.write("--- Dump Table Structure:" + table + "---\n");
        fileWriter.write(this.dropTableSql.replace("{table}", table) + ";\n");
        // select ddl
        String sql = this.ddlSql.replace("{table}", table);
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            fileWriter.write(rs.getString(2) + ";\n");
        }
        rs.close();
        stmt.close();
    }

    private void dumpTableData(String table, FileWriter fileWriter, ExcelWriter excelWriter, WriteSheet writeSheet)
            throws Exception {
        if (this.fileType.equals("sql")) {
            fileWriter.write("--- Dump Table Data:" + table + "---\n");
        }
        // 查询数据个数
        String sql = "SELECT COUNT(*) FROM  " + table;
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            long count = rs.getLong(1);
            if (this.fileType.equals("sql")) {
                fileWriter.write("--- Total:" + count + "---\n");
            }
            rs.close();
            stmt.close();
            //
            long pageSize = 1000;
            if (count <= pageSize) {
                dumpTableDataPage(table, fileWriter, excelWriter, writeSheet, 0, count);
            } else {
                long pageCount = count / pageSize;
                for (int i = 0; i < pageCount; i++) {
                    dumpTableDataPage(table, fileWriter, excelWriter, writeSheet, i * pageSize, pageSize);
                }
            }
        }
    }

    private void dumpTableDataPage(String table, FileWriter fileWriter, ExcelWriter excelWriter,
            WriteSheet writeSheet,
            long start,
            long length) throws Exception {

        if (this.fileType.equals("sql")) {
            fileWriter.write("--- Page:" + start + "," + length + "---\n");
        }
        // 查询数据
        String sql = "SELECT * FROM  " + table + " "
                + this.pageSql.replace("{1}", start + "").replace("{2}", length + "");
        java.sql.Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql);
        JSONArray supportFieldTypes = JSONArray.parseArray(this.fieldTypes);
        List<List<Object>> rows = null;
        if (this.fileType.equals("xlsx")) {
            rows = new ArrayList<>();
        }
        new ArrayList<>();
        long index = start;
        while (rs.next()) {
            if (this.fileType.equals("sql")) {
                dumpTableDataPageSql(table, rs, fileWriter, supportFieldTypes);
            } else if (this.fileType.equals("xlsx")) {
                rows.add(dumpTableDataPageExcel(table, rs, excelWriter, writeSheet, supportFieldTypes, index));
            } else if (this.fileType.equals("csv")) {
                dumpTableDataPageCsv(table, rs, fileWriter, supportFieldTypes, index);
            } else if (this.fileType.equals("json")) {
                dumpTableDataPageJSON(table, rs, fileWriter, supportFieldTypes, index);
            }
            index++;
        }
        if (excelWriter != null) {
            excelWriter.write(rows, writeSheet);
        }
        if (fileWriter != null) {
            fileWriter.flush();
        }
        rs.close();
        stmt.close();
    }

    private void dumpTableDataPageSql(String table, java.sql.ResultSet rs, FileWriter fileWriter,
            JSONArray supportFieldTypes)
            throws Exception {
        // insert into
        fileWriter.write("INSERT INTO `" + table + "` (");
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            fileWriter.write(this.identifierQuoteSymbol + rs.getMetaData().getColumnName(i)
                    + this.identifierQuoteSymbol);
            if (i < rs.getMetaData().getColumnCount()) {
                fileWriter.write(",");
            }
        }
        fileWriter.write(") VALUES (");
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            String typeName = rs.getMetaData().getColumnTypeName(i);
            if (rs.getString(i) == null) {
                fileWriter.write("NULL");
            } else {
                String catalog = "String";
                for (int j = 0; j < supportFieldTypes.size(); j++) {
                    JSONObject fieldType = supportFieldTypes.getJSONObject(j);
                    if (fieldType.getString("name").equals(typeName)) {
                        catalog = fieldType.getString("catalog");
                        break;
                    }
                }
                if ("Integer Fixed-Point Floating-Point  Binary  Bit-Value Enumeration".indexOf(catalog) >= 0) {
                    fileWriter.write(rs.getString(i));
                } else {
                    fileWriter.write("'" + rs.getString(i).replace("'", "''") + "'");
                }
            }
            if (i < rs.getMetaData().getColumnCount()) {
                fileWriter.write(",");
            }
        }
        fileWriter.write(");\n");
    }

    private List<Object> dumpTableDataPageExcel(String table, java.sql.ResultSet rs, ExcelWriter excelWriter,
            WriteSheet writeSheet,
            JSONArray supportFieldTypes, long start)
            throws Exception {
        // insert into
        if (start == 0) {
            // 插入表头
            List<List<String>> headList = new ArrayList<List<String>>();
            List<String> head = new ArrayList<String>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
              head.add(rs.getMetaData().getColumnName(i));
            }
            headList.add(head);
            excelWriter.write(headList, writeSheet);
        }
        // 插入数据
        List<Object> data = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            data.add(rs.getObject(i));
        }
        return data;
    }

    private void dumpTableDataPageCsv(String table, java.sql.ResultSet rs, FileWriter fileWriter,
            JSONArray supportFieldTypes, long start)
            throws Exception {
        // 插入表头
        if (start == 0) {

            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                fileWriter.write(rs.getMetaData().getColumnName(i));
                if (i < rs.getMetaData().getColumnCount()) {
                    fileWriter.write(",");
                }
            }
            fileWriter.write("\n");
        }
        // 插入数据
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            fileWriter.write(rs.getObject(i).toString());
            if (i < rs.getMetaData().getColumnCount()) {
                fileWriter.write(",");
            }
        }
        fileWriter.write("\n");
    }

    private void dumpTableDataPageJSON(String table, java.sql.ResultSet rs, FileWriter fileWriter,
            JSONArray supportFieldTypes, long start)
            throws Exception {
        Map<String, Object> data = new HashMap<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            data.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
        }
        fileWriter.write(JSON.toJSONString(data));
        fileWriter.write(",\n");
    }

}

class DateConverter implements Converter<java.sql.Date> {
    @Override
    public Class<java.sql.Date> supportJavaTypeKey() {
        return java.sql.Date.class;
    }

    @Override
    public WriteCellData<?> convertToExcelData(java.sql.Date value, ExcelContentProperty property,
            GlobalConfiguration config) {
        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (value == null) {
            return new WriteCellData<>("");
        }
        return new WriteCellData<>(value.toString());
    }
}
class TimestampConverter implements Converter<java.sql.Timestamp> {
    @Override
    public Class<java.sql.Timestamp> supportJavaTypeKey() {
        return java.sql.Timestamp.class;
    }
    @Override
    public WriteCellData<?> convertToExcelData(java.sql.Timestamp value, ExcelContentProperty property,
            GlobalConfiguration config) {
        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (value == null) {
            return new WriteCellData<>("");
        }
        return new WriteCellData<>(value.toString());
    }
}
class TimeConverter implements Converter<java.sql.Time> {
    @Override
    public Class<java.sql.Time> supportJavaTypeKey() {
        return java.sql.Time.class;
    }
    @Override
    public WriteCellData<?> convertToExcelData(java.sql.Time value, ExcelContentProperty property,
            GlobalConfiguration config) {
        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (value == null) {
            return new WriteCellData<>("");
        }
        return new WriteCellData<>(value.toString());
    }
}