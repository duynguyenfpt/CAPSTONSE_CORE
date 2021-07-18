package models;

import java.sql.Date;

public class CDCModel {
    private int id;
    private String database_url;
    private String database_port;
    private String database_name;
    private String table_name;
    private String schema;
    private String value;
    private int operation;
    private long created;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDatabase_url() {
        return database_url;
    }

    public void setDatabase_url(String database_url) {
        this.database_url = database_url;
    }

    public String getDatabase_port() {
        return database_port;
    }

    public void setDatabase_port(String database_port) {
        this.database_port = database_port;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getOperation() {
        return operation;
    }

    public void setOperation(int operation) {
        this.operation = operation;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public CDCModel(int id, String database_url, String database_port, String database_name, String table_name, String schema, String value, int operation, long created) {
        this.id = id;
        this.database_url = database_url;
        this.database_port = database_port;
        this.database_name = database_name;
        this.table_name = table_name;
        this.schema = schema;
        this.value = value;
        this.operation = operation;
        this.created = created;
    }

    public CDCModel() {
    }
}
