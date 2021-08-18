package models;

public class ReadinessModel {
    private String host;
    private String port;
    private String database;
    private String table;
    private int isProcess;

    public int getIsProcess() {
        return isProcess;
    }

    public void setIsProcess(int isProcess) {
        this.isProcess = isProcess;
    }

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

    public ReadinessModel() {
    }

    @Override
    public String toString() {
        return "ReadinessModel{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", isProcess=" + isProcess +
                '}';
    }
}
