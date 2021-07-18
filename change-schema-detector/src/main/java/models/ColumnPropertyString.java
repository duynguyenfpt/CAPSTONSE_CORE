package models;

import java.util.ArrayList;
import java.util.List;

public class ColumnPropertyString implements Comparable<ColumnPropertyString> {
    private String field;
    private String type;
    private Integer typeSize = -1;
    private Boolean isSigned;
    // must be sorted
    private String defaultValue = "NULL";
    private String possibleValues = "";

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getTypeSize() {
        return typeSize;
    }

    public void setTypeSize(Integer typeSize) {
        this.typeSize = typeSize;
    }

    public boolean isSigned() {
        return isSigned;
    }

    public void setSigned(Boolean signed) {
        isSigned = signed;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Boolean getSigned() {
        return isSigned;
    }

    public String getPossibleValues() {
        return possibleValues;
    }

    public void setPossibleValues(String possibleValues) {
        this.possibleValues = possibleValues;
    }

    public ColumnPropertyString() {
    }

    public ColumnPropertyString(String field, String type, Integer typeSize, Boolean isSigned, String defaultValue, String possibleValues) {
        this.field = field;
        this.type = type;
        this.typeSize = typeSize;
        this.isSigned = isSigned;
        this.defaultValue = defaultValue;
        this.possibleValues = possibleValues;
    }

    @Override
    public String toString() {
        return "ColumnProperty{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", typeSize=" + typeSize +
                ", isSigned=" + isSigned +
                ", defaultValue='" + defaultValue + '\'' +
                ", possibleValues=" + possibleValues +
                '}';
    }

    @Override
    public int compareTo(ColumnPropertyString o) {
        return this.getField().compareToIgnoreCase(o.getField());
    }
}
