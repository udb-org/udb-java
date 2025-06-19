package com.udb.server.service.thread;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.mozilla.universalchardet.UniversalDetector;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
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
public class ImportThread extends ComThread {

    private JSONObject datasourceJson;
    private String path;
    private String mapping;
    private boolean isClear;
    private String table;
    private String clearTableSql;
    private String identifierQuoteSymbol;

    public ImportThread(String sessionId, Map<String, Object> body) {
        System.out.println("sessionId: " + sessionId);
        this.sessionId = sessionId;
        this.datasourceJson = JSONObject.parseObject(body.get("datasource").toString());
        this.path = body.get("path").toString();
        this.mapping = body.get("mapping").toString();
        this.isClear = body.get("isClear").toString().equals("true");
        this.table = body.get("table").toString();
    }

    public String getType() {
        return "dump";
    }

    public String getLable() {
        return datasourceJson.getString("name");
    }

    @Override
    public void run() {

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
            this.conn.setAutoCommit(false);
            this.isTransaction = true;
            // save sql
            if (this.isClear) {
                clearTable();
            }
            java.sql.Statement stmt = conn.createStatement();

            List<String> sqls = null;
            if (this.path.endsWith(".xlsx")) {
                ImportXlsxListener listener = new ImportXlsxListener(stmt, this.mapping, this.table,
                        this.identifierQuoteSymbol);
                EasyExcel.read(this.path, listener).sheet().doRead();

            } else if (this.path.endsWith(".csv")) {
                importCsv(stmt, this.mapping, this.table, this.identifierQuoteSymbol);
            } else if (this.path.endsWith(".json")) {
                importJson(stmt, this.mapping, this.table, this.identifierQuoteSymbol);
            } else {
                status = 500;
                message = "File format is invalid";
                endTime = new java.util.Date();
                return;
            }
            if (sqls == null) {
                status = 500;
                message = "File Read Error";
                endTime = new java.util.Date();
                return;
            }

            stmt.executeBatch();
            stmt.close();
            this.conn.commit();
            this.conn.close();
            this.status = 200;
            this.message = "Execute success";
            endTime = new java.util.Date();
        } catch (Exception e) {
            try {
                if (conn != null) {
                    this.conn.rollback();
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

    private void clearTable() throws Exception {
        String sql = this.clearTableSql;
        java.sql.Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    private String detectFileEncoding(File file) throws IOException {
        byte[] buf = new byte[4096];
        UniversalDetector detector = new UniversalDetector(null);
        try (FileInputStream fis = new FileInputStream(file)) {
            int nread;
            while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
                detector.handleData(buf, 0, nread);
            }
            detector.dataEnd();
        }
        String encoding = detector.getDetectedCharset();
        detector.reset();
        return encoding != null ? encoding : "UTF-8";
    }

    private void importCsv(java.sql.Statement stmt, String mapping, String table, String identifierQuoteSymbol)
            throws Exception {
        String delimiter = ",";
        File file = new File(path);
        String encoding = detectFileEncoding(file);

        JSONArray mappingArray = JSONArray.parseArray(mapping);

        // 构建INSERT SQL前缀
        StringBuilder insertSql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (int i = 0; i < mappingArray.size(); i++) {
            JSONObject mappingObj = mappingArray.getJSONObject(i);
            insertSql.append(identifierQuoteSymbol).append(mappingObj.getString("name")).append(identifierQuoteSymbol);
            if (i < mappingArray.size() - 1) {
                insertSql.append(",");
            }
        }
        insertSql.append(") VALUES (");

        // 使用OpenCSV读取CSV文件
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(delimiter.charAt(0))
                .build();
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(file), encoding))
                .withCSVParser(parser)
                .build()) {
            String[] headers = reader.readNext(); // 读取表头
            String[] line;
            int batchSize = 1000;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                StringBuilder values = new StringBuilder();
                for (int i = 0; i < mappingArray.size(); i++) {
                    JSONObject mappingObj = mappingArray.getJSONObject(i);
                    int csvIndex = mappingObj.getIntValue("index");
                    String value = line.length > csvIndex ? line[csvIndex] : "";
                    // 处理SQL注入和特殊字符
                    values.append("'").append(value.replace("'", "''")).append("'");
                    if (i < mappingArray.size() - 1) {
                        values.append(",");
                    }
                }

                stmt.addBatch(insertSql.toString() + values + ")");
                count++;

                if (count >= batchSize) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    count = 0;
                }
            }

            if (count > 0) {
                stmt.executeBatch();
            }
        }
    }

    private void importJson(java.sql.Statement stmt, String mapping, String table, String identifierQuoteSymbol) throws Exception {
        File file = new File(path);
        String encoding = detectFileEncoding(file);
        JSONArray mappingArray = JSONArray.parseArray(mapping);
        
        // 构建INSERT SQL前缀
        StringBuilder insertSql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (int i = 0; i < mappingArray.size(); i++) {
            JSONObject mappingObj = mappingArray.getJSONObject(i);
            insertSql.append(identifierQuoteSymbol).append(mappingObj.getString("name")).append(identifierQuoteSymbol);
            if (i < mappingArray.size() - 1) {
                insertSql.append(",");
            }
        }
        insertSql.append(") VALUES (");
        
        int batchSize = 1000;
        int count = 0;
        
        // 使用FastJSON读取JSON文件
        String jsonContent = new String(Files.readAllBytes(file.toPath()), encoding);
        
        // 尝试解析为JSON数组
        if (jsonContent.trim().startsWith("[")) {
            JSONArray jsonArray = JSONArray.parseArray(jsonContent);
            for (Object obj : jsonArray) {
                if (obj instanceof JSONObject) {
                    count = processJsonObject((JSONObject) obj, mappingArray, insertSql, stmt, batchSize, count);
                }
            }
        } 
        // 解析为单个JSON对象
        else if (jsonContent.trim().startsWith("{")) {
            JSONObject jsonObject = JSONObject.parseObject(jsonContent);
            count = processJsonObject(jsonObject, mappingArray, insertSql, stmt, batchSize, count);
        } else {
            throw new Exception("Invalid JSON format");
        }
        
        // 执行剩余批次
        if (count > 0) {
            stmt.executeBatch();
        }
    }
    
    private int processJsonObject(JSONObject jsonObject, JSONArray mappingArray, StringBuilder insertSql, 
                                 java.sql.Statement stmt, int batchSize, int count) throws Exception {
        // 构建VALUES部分
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < mappingArray.size(); i++) {
            JSONObject mappingObj = mappingArray.getJSONObject(i);
            String fieldName = mappingObj.getString("name");
            Object value = jsonObject.get(fieldName);
            
            // 处理空值和字符串转义
            if (value == null) {
                values.append("NULL");
            } else {
                String strValue = value.toString().replace("'", "''");
                values.append("'").append(strValue).append("'");
            }
            
            if (i < mappingArray.size() - 1) {
                values.append(",");
            }
        }
        
        stmt.addBatch(insertSql.toString() + values + ")");
        count++;
        
        // 达到批次大小执行批量插入
        if (count >= batchSize) {
            stmt.executeBatch();
            stmt.clearBatch();
            count = 0;
        }
        
        return count;
    }

}

class ImportXlsxListener implements ReadListener<Map<Integer, String>> {
    private List<String> list;
    private java.sql.Statement stmt;
    /**
     * table: string;
     * name: string;
     * type: string;
     * displayType?: string;
     * comment?: string;
     * isNullable?: boolean;
     * defaultValue?: string | number | boolean | null;
     * autoIncrement?: boolean;
     * length?: number;
     * scale?: number;
     * position?: number;
     * index:number;
     * head:string;
     * catalog: FieldTypeCategory;
     */
    private JSONArray mappingArray;
    private String insertSql;

    public ImportXlsxListener(java.sql.Statement stmt, String mapping, String table, String identifierQuoteSymbol) {
        this.stmt = stmt;
        this.mappingArray = JSONArray.parseArray(mapping);

        this.insertSql = "insert into " + table + "(";
        for (int i = 0; i < mappingArray.size(); i++) {
            JSONObject mappingObj = mappingArray.getJSONObject(i);
            String name = mappingObj.getString("name");
            insertSql += identifierQuoteSymbol + name + identifierQuoteSymbol;
            if (i < mappingArray.size() - 1) {
                insertSql += ",";
            }
        }
        insertSql += ") values (";
    }

    public List<String> getList() {
        return list;
    }

    @Override
    public void invoke(Map<Integer, String> data, AnalysisContext context) {
        // 判断是否是第一行
        if (context.readRowHolder().getRowIndex() == 0) {

        } else if (data.size() > 0) {
            StringBuffer sql = new StringBuffer(insertSql);
            for (int i = 0; i < mappingArray.size(); i++) {
                JSONObject mappingObj = mappingArray.getJSONObject(i);
                int index = mappingObj.getInteger("index");
                String val = null;
                if (data.containsKey(index)) {
                    val = data.get(index);
                }
                String catalog = mappingObj.getString("catalog");
                if ("Integer Fixed-Point Floating-Point  Binary  Bit-Value Enumeration".indexOf(catalog) >= 0) {
                    sql.append(val);
                } else {
                    sql.append("'" + val + "'");
                }
                if (i < mappingArray.size() - 1) {
                    sql.append(",");
                }
            }

            try {
                this.stmt.addBatch(sql.toString());
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {

    }

}