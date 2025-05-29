package com.udb.server.bodies;


public class ExeSqlBody {
    private String id;
    private String sql;
    private String datasource;
    private boolean isTransaction;
    public boolean isTransaction() {
        return isTransaction;
    }
    public void setTransaction(boolean isTransaction) {
        this.isTransaction = isTransaction;
    }
    public String getSql() {
        return sql;
    }
    public void setSql(String sql) {
        this.sql = sql;
    }
    public String getDatasource() {
        return datasource;
    }
    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    } 
}
