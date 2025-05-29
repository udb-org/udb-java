package com.udb.server.bodies;

public class DumpDatabaseBody {
    private String datasource;
    private String path;
    private String tables;
    private String dumpType;

    public String getDatasource() {
        return datasource;
    }
    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getTables() {
        return tables;
    }
    public void setTables(String tables) {
        this.tables = tables;
    }
    public String getDumpType() {
        return dumpType;
    }
    public void setDumpType(String dumpType) {
        this.dumpType = dumpType;
    }

    

  
    
}
