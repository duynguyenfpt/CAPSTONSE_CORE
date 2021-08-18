package models;

public class TableModel {
    private int table_id;
    private String table;
    private String database_alias;

    public int getTable_id() {
        return table_id;
    }

    public void setTable_id(int table_id) {
        this.table_id = table_id;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDatabase_alias() {
        return database_alias;
    }

    public void setDatabase_alias(String database_alias) {
        this.database_alias = database_alias;
    }

    public TableModel(int table_id, String table, String database_alias) {
        this.table_id = table_id;
        this.table = table;
        this.database_alias = database_alias;
    }

    public TableModel() {
    }
}
