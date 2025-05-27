package com.udb.server.bodies;

public class ExportDataBody {
    // private  databases Map
    private String data;
    private String path;
    private String format;
    public String getFormat() {
        return format;
    }
    public void setFormat(String format) {
        this.format = format;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    
}
