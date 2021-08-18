package models;

public class MergeRequestModel {
    private String host;
    private String port;
    private String databaseType;
    private String database;
    private String table;
    private String username;
    private String mergeTable;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getMergeTable() {
        return mergeTable;
    }

    public void setMergeTable(String mergeTable) {
        this.mergeTable = mergeTable;
    }


}
