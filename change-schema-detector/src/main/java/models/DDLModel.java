package models;

public class DDLModel {
    private String dbType;
    private String dbTable;
    private String fieldChanged;
    private String changeType;
    private String oldValue;
    private String newValue;

    public DDLModel() {
    }

    public DDLModel(String dbType, String dbTable, String fieldChanged, String changeType, String oldValue, String newValue) {
        this.dbType = dbType;
        this.dbTable = dbTable;
        this.fieldChanged = fieldChanged;
        this.changeType = changeType;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public DDLModel(String dbType, String dbTable) {
        this.dbType = dbType;
        this.dbTable = dbTable;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbTable() {
        return dbTable;
    }

    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

    public String getFieldChanged() {
        return fieldChanged;
    }

    public void setFieldChanged(String fieldChanged) {
        this.fieldChanged = fieldChanged;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }



    @Override
    public String toString() {
        return "DDLModel{" +
                "dbType='" + dbType + '\'' +
                ", dbTable='" + dbTable + '\'' +
                ", fieldChanged='" + fieldChanged + '\'' +
                ", changeType='" + changeType + '\'' +
                ", oldValue='" + oldValue + '\'' +
                ", newValue='" + newValue + '\'' +
                '}';
    }
}
