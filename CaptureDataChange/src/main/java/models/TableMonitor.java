package models;

public class TableMonitor {
    private String server_host;
    private String port;
    private String database_type;
    private String identity_key;
    private String partition_by;
    private int str_id;
    private int job_id;
    private int max_retries;
    private int number_retries;
    private int latest_offset;
    private String table;
    private String database;
    private String username;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getServer_host() {
        return server_host;
    }

    public void setServer_host(String server_host) {
        this.server_host = server_host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabase_type() {
        return database_type;
    }

    public void setDatabase_type(String database_type) {
        this.database_type = database_type;
    }

    public String getIdentity_key() {
        return identity_key;
    }

    public void setIdentity_key(String identity_key) {
        this.identity_key = identity_key;
    }

    public String getPartition_by() {
        return partition_by;
    }

    public void setPartition_by(String partition_by) {
        this.partition_by = partition_by;
    }

    public int getStr_id() {
        return str_id;
    }

    public void setStr_id(int str_id) {
        this.str_id = str_id;
    }

    public int getJob_id() {
        return job_id;
    }

    public void setJob_id(int job_id) {
        this.job_id = job_id;
    }

    public int getMax_retries() {
        return max_retries;
    }

    public void setMax_retries(int max_retries) {
        this.max_retries = max_retries;
    }

    public int getNumber_retries() {
        return number_retries;
    }

    public void setNumber_retries(int number_retries) {
        this.number_retries = number_retries;
    }

    public int getLatest_offset() {
        return latest_offset;
    }

    public void setLatest_offset(int latest_offset) {
        this.latest_offset = latest_offset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "TableMonitor{" +
                "server_host='" + server_host + '\'' +
                ", port='" + port + '\'' +
                ", database_type='" + database_type + '\'' +
                ", identity_key='" + identity_key + '\'' +
                ", partition_by='" + partition_by + '\'' +
                ", str_id=" + str_id +
                ", job_id=" + job_id +
                ", max_retries=" + max_retries +
                ", number_retries=" + number_retries +
                ", latest_offset=" + latest_offset +
                ", table='" + table + '\'' +
                '}';
    }
}
